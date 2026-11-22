package handler

import (
	"errors"
	"net/http"

	"shorturl/internal/logic"

	"github.com/zeromicro/go-zero/core/logx"
)

func writeLogicError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, logic.ErrInvalidLongURL):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, logic.ErrShortURLNotFound):
		http.Error(w, logic.ErrShortURLNotFound.Error(), http.StatusNotFound)
	default:
		logx.WithContext(r.Context()).Errorf("%s %s failed: %v", r.Method, r.URL.Path, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}
