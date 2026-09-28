package backup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"iedb/internal/storage"
)

// BackupOptions controls what gets backed up and where.
type BackupOptions struct {
	IncludeMetadata bool // back up the SQLite database
	IncludeConfig   bool // back up iedb.toml
}

// BackupResult is returned when a backup completes.
type BackupResult struct {
	Manifest *Manifest
	Duration time.Duration
}

// CreateBackup performs a full backup. It runs synchronously; the API layer
// launches it in a goroutine and exposes progress via GetProgress().
func (m *Manager) CreateBackup(ctx context.Context, opts BackupOptions) (*BackupResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	backupID := generateBackupID()
	startTime := time.Now()

	progress := &Progress{
		Operation: "backup",
		BackupID:  backupID,
		Status:    "running",
		StartedAt: startTime,
	}
	m.setProgress(progress)
	defer func() {
		now := time.Now()
		progress.CompletedAt = &now
		m.setProgress(progress)
	}()

	m.logger.Info().Str("backup_id", backupID).Msg("Starting backup")

	// ── 1. Discover data files ──────────────────────────────────────────
	objectLister, ok := m.dataStorage.(storage.ObjectLister)
	if !ok {
		progress.Status = "failed"
		progress.Error = "storage backend does not support listing objects"
		return nil, fmt.Errorf("storage backend does not support ListObjects")
	}

	objects, err := objectLister.ListObjects(ctx, "")
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to list data files: %w", err)
	}

	// Filter to .parquet files only
	var parquetFiles []storage.ObjectInfo
	for _, obj := range objects {
		if strings.HasSuffix(obj.Path, ".parquet") {
			parquetFiles = append(parquetFiles, obj)
		}
	}

	// Build manifest inventory
	manifest := &Manifest{
		Version:    "dev",
		BackupID:   backupID,
		CreatedAt:  startTime.UTC(),
		BackupType: "full",
	}

	dbMap := make(map[string]*DatabaseInfo)
	for _, obj := range parquetFiles {
		manifest.TotalFiles++
		manifest.TotalSizeBytes += obj.Size

		db, meas := parseDBMeasurement(obj.Path)
		di, exists := dbMap[db]
		if !exists {
			di = &DatabaseInfo{Name: db}
			dbMap[db] = di
		}
		di.FileCount++
		di.SizeBytes += obj.Size

		// Find or create measurement entry
		found := false
		for i := range di.Measurements {
			if di.Measurements[i].Name == meas {
				di.Measurements[i].FileCount++
				di.Measurements[i].SizeBytes += obj.Size
				found = true
				break
			}
		}
		if !found {
			di.Measurements = append(di.Measurements, MeasurementInfo{
				Name:      meas,
				FileCount: 1,
				SizeBytes: obj.Size,
			})
		}
	}
	for _, di := range dbMap {
		manifest.Databases = append(manifest.Databases, *di)
	}

	progress.TotalFiles = manifest.TotalFiles
	progress.TotalBytes = manifest.TotalSizeBytes

	// ── 2. Copy parquet files ───────────────────────────────────────────
	if err := m.copyDataFiles(ctx, backupID, parquetFiles, progress); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// ── 3. Copy SQLite metadata ─────────────────────────────────────────
	if opts.IncludeMetadata && m.sqliteDBPath != "" {
		if err := m.backupSQLite(ctx, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup SQLite database")
			// Non-fatal: continue with backup
		} else {
			manifest.HasMetadata = true
		}
	}

	// ── 4. Copy config ──────────────────────────────────────────────────
	if opts.IncludeConfig && m.configPath != "" {
		if err := m.backupConfig(ctx, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup config file")
		} else {
			manifest.HasConfig = true
		}
	}

	// ── 5. Write manifest ───────────────────────────────────────────────
	manifestData, err := MarshalManifest(manifest)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}
	manifestPath := fmt.Sprintf("%s/manifest.json", backupID)
	if err := m.backupStorage.Write(ctx, manifestPath, manifestData); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}

	progress.Status = "completed"
	duration := time.Since(startTime)

	m.logger.Info().
		Str("backup_id", backupID).
		Int64("files", manifest.TotalFiles).
		Int64("bytes", manifest.TotalSizeBytes).
		Dur("duration", duration).
		Msg("Backup completed")

	return &BackupResult{Manifest: manifest, Duration: duration}, nil
}

// copyDataFiles copies parquet files from data storage to backup storage.
func (m *Manager) copyDataFiles(ctx context.Context, backupID string, files []storage.ObjectInfo, progress *Progress) error {
	for _, obj := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		srcData, err := m.dataStorage.Read(ctx, obj.Path)
		if err != nil {
			m.logger.Warn().Str("path", obj.Path).Err(err).Msg("Failed to read data file, skipping")
			continue
		}

		destPath := fmt.Sprintf("%s/data/%s", backupID, obj.Path)
		if err := m.backupStorage.Write(ctx, destPath, srcData); err != nil {
			return fmt.Errorf("failed to write backup file %s: %w", destPath, err)
		}

		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, obj.Size)

		if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
			m.logger.Info().
				Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
				Int64("total", progress.TotalFiles).
				Msg("Backup progress")
		}
	}
	return nil
}

// partSuffix mirrors the staging suffix LocalBackend.WriteReader uses for
// in-progress writes. Kept in sync with internal/storage/local.go#partPath.
const partSuffix = ".part"

// cleanupPartialWrite removes the staging file a failed WriteReader leaves behind.
//
// LocalBackend.WriteReader deliberately preserves "<path>.part" on failure so the
// file-replication puller can resume from the last committed byte. Backup has no
// resume path — a retried backup starts over under a fresh backup ID — so that
// staging file is unreferenced garbage: never read, never listed as a backup
// (it has no manifest.json), and holding disk equal to the bytes transferred
// before the failure.
//
// Best-effort by design: the write already failed, so the cleanup very likely
// fails too (unwritable volume, storage unreachable). A cleanup failure must not
// mask the real error, so it is logged at debug and discarded.
func (m *Manager) cleanupPartialWrite(ctx context.Context, destPath string) {
	stagingPath := destPath + partSuffix
	if err := m.backupStorage.Delete(ctx, stagingPath); err != nil {
		m.logger.Debug().
			Str("path", stagingPath).
			Err(err).
			Msg("Could not remove partial backup staging file")
	}
}

// backupSQLite copies the shared SQLite database into the backup.
func (m *Manager) backupSQLite(ctx context.Context, backupID string) error {
	return m.backupSQLiteFile(ctx, backupID, m.sqliteDBPath, "iedb.db")
}

// backupSQLiteFile copies one SQLite database file into the backup under
// metadata/<destName>. It performs a WAL checkpoint first so the copy is
// consistent, and streams via the storage backend rather than reading the whole
// database into memory.
func (m *Manager) backupSQLiteFile(ctx context.Context, backupID, dbPath, destName string) error {
	// Checkpoint WAL to ensure all data is flushed to the main DB file.
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite for checkpoint: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		db.Close()
		return fmt.Errorf("WAL checkpoint failed: %w", err)
	}
	db.Close()

	// Get file size for WriteReader
	info, err := os.Stat(dbPath)
	if err != nil {
		return fmt.Errorf("failed to stat SQLite database: %w", err)
	}
	size := info.Size()

	// Stream via temp file to avoid loading entire DB into memory
	f, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}
	defer f.Close()

	destPath := fmt.Sprintf("%s/metadata/%s", backupID, destName)
	if err := m.backupStorage.WriteReader(ctx, destPath, f, size); err != nil {
		return fmt.Errorf("failed to write SQLite backup: %w", err)
	}

	m.logger.Info().
		Str("backup_id", backupID).
		Str("database", destName).
		Int64("bytes", size).
		Msg("SQLite database backed up")
	return nil
}

// backupConfig copies the iedb.toml config file into the backup.
//
// SECURITY: iedb.toml typically contains plaintext credentials (S3 secret key,
// Azure account key, cluster shared secret). The backup storage must be treated
// as secret material with the same access controls as the live config.
// Operators who cannot secure backup storage should set backup.include_config
// to false in iedb.toml.
func (m *Manager) backupConfig(ctx context.Context, backupID string) error {
	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	destPath := fmt.Sprintf("%s/config/iedb.toml", backupID)
	if err := m.backupStorage.Write(ctx, destPath, data); err != nil {
		return fmt.Errorf("failed to write config backup: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file backed up")
	return nil
}

// parseDBMeasurement extracts the database and measurement from a storage path.
// Path format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.parquet
func parseDBMeasurement(path string) (database, measurement string) {
	path = filepath.ToSlash(path)
	parts := strings.SplitN(path, "/", 3)
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	if len(parts) == 1 {
		return parts[0], "unknown"
	}
	return "unknown", "unknown"
}
