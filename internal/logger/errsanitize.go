package logger

import (
	sqlutil "iedb/internal/sql"

	"github.com/rs/zerolog"
)

func installErrSanitizer() {
	zerolog.ErrorMarshalFunc = func(err error) interface{} {
		if err == nil {
			return nil
		}
		return sanitizeErrText(err.Error())
	}
}

func sanitizeErrText(s string) string { return sqlutil.SanitizeErrText(s) }
