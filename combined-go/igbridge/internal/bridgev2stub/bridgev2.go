// Stand-in for the few maunium.net/go/mautrix/bridgev2 login types that the
// vendored upstream login flows reference. MirrorMsg never runs those flows (it logs
// in with cookies from a WebView), but keeping them compiling lets the upstream files
// stay unchanged apart from their import paths.
//
// Type definitions copied from mautrix-go (bridgev2/login.go, bridgev2/errors.go and
// error.go at commit f14e6b5fc009), Mozilla Public License 2.0, see LICENSE.

package bridgev2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/rs/zerolog"

	"fi.mirrormsg/igbridge/internal/eventstub"
)

type RespError struct {
	ErrCode   string
	Err       string
	ExtraData map[string]any

	StatusCode  int
	ExtraHeader map[string]string

	CanRetry      bool
	OAuth         bool
	InternalError string
}

func (re RespError) Error() string {
	return re.Err
}

func (re RespError) Is(err error) bool {
	var e2 RespError
	if errors.As(err, &e2) {
		return e2.Err == re.Err
	}
	return false
}

func (re RespError) Write(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(re.StatusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{"errcode": re.ErrCode, "error": re.Err})
}

func (re RespError) WithInternalError(err error) RespError {
	re.InternalError = err.Error()
	return re
}

func (re RespError) WithMessage(msg string, args ...any) RespError {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	re.Err = msg
	return re
}

func (re RespError) AppendMessage(append string, args ...any) RespError {
	re.Err += fmt.Sprintf(append, args...)
	return re
}

func WrapRespErrManual(err error, code string, status int) RespError {
	return RespError{ErrCode: code, Err: err.Error(), StatusCode: status}
}

var ErrLoginStepCancelled = RespError{
	ErrCode:    "FI.MAU.LOGIN_STEP_CANCELLED",
	Err:        "Login step was cancelled",
	StatusCode: http.StatusConflict,
}

type LoginStepType string

const (
	LoginStepTypeUserInput      LoginStepType = "user_input"
	LoginStepTypeCookies        LoginStepType = "cookies"
	LoginStepTypeClientHTTP     LoginStepType = "client_http"
	LoginStepTypeDisplayAndWait LoginStepType = "display_and_wait"
	LoginStepTypeWebAuthn       LoginStepType = "webauthn"
	LoginStepTypeComplete       LoginStepType = "complete"
)

type LoginDisplayType string

const (
	LoginDisplayTypeQR      LoginDisplayType = "qr"
	LoginDisplayTypeEmoji   LoginDisplayType = "emoji"
	LoginDisplayTypeCode    LoginDisplayType = "code"
	LoginDisplayTypeNothing LoginDisplayType = "nothing"
)

type LoginStep struct {
	Type         LoginStepType `json:"type"`
	StepID       string        `json:"step_id"`
	TxnID        string        `json:"txn_id"`
	Instructions string        `json:"instructions"`

	DisplayAndWaitParams *LoginDisplayAndWaitParams `json:"display_and_wait,omitempty"`
	CookiesParams        *LoginCookiesParams        `json:"cookies,omitempty"`
	ClientHTTPParams     *LoginClientHTTPParams     `json:"client_http,omitempty"`
	UserInputParams      *LoginUserInputParams      `json:"user_input,omitempty"`
	WebAuthnParams       *LoginWebAuthnParams       `json:"webauthn,omitempty"`
	CompleteParams       *LoginCompleteParams       `json:"complete,omitempty"`
}

func (ls *LoginStep) MarshalZerologObject(e *zerolog.Event) {
	e.Str("type", string(ls.Type)).
		Str("step_id", ls.StepID).
		Str("txn_id", ls.TxnID).
		Str("instructions", ls.Instructions)
}

type LoginClientHTTPParams struct {
	RequestID string      `json:"request_id"`
	Method    string      `json:"method"`
	URL       string      `json:"url"`
	Headers   http.Header `json:"headers,omitempty"`
	Body      []byte      `json:"body,omitempty"`

	Fingerprint string `json:"fingerprint,omitempty"`
}

func (lchp *LoginClientHTTPParams) MarshalZerologObject(e *zerolog.Event) {
	e.Str("request_id", lchp.RequestID).
		Str("method", lchp.Method).
		Str("url", lchp.URL).
		Int("header_count", len(lchp.Headers)).
		Int("body_length", len(lchp.Body)).
		Str("fingerprint", lchp.Fingerprint)
}

type LoginClientHTTPResponse struct {
	StatusCode  int         `json:"status_code,omitzero"`
	FinalURL    string      `json:"final_url,omitempty"`
	Headers     http.Header `json:"headers,omitempty"`
	Body        []byte      `json:"body,omitempty"`
	Error       string      `json:"error,omitempty"`
	Fingerprint string      `json:"fingerprint,omitempty"`
}

type LoginWebAuthnParams struct {
	URL       string          `json:"url,omitempty"`
	PublicKey json.RawMessage `json:"publicKey,omitempty"`
}

type LoginDisplayAndWaitParams struct {
	Type      LoginDisplayType `json:"type"`
	Data      string           `json:"data,omitempty"`
	ImageURL  string           `json:"image_url,omitempty"`
	CanCancel bool             `json:"can_cancel,omitzero"`
}

type LoginCookieFieldSourceType string

const (
	LoginCookieTypeCookie        LoginCookieFieldSourceType = "cookie"
	LoginCookieTypeLocalStorage  LoginCookieFieldSourceType = "local_storage"
	LoginCookieTypeRequestHeader LoginCookieFieldSourceType = "request_header"
	LoginCookieTypeRequestBody   LoginCookieFieldSourceType = "request_body"
	LoginCookieTypeSpecial       LoginCookieFieldSourceType = "special"
)

type LoginCookieFieldSource struct {
	Type            LoginCookieFieldSourceType `json:"type"`
	Name            string                     `json:"name"`
	RequestURLRegex string                     `json:"request_url_regex,omitempty"`
	CookieDomain    string                     `json:"cookie_domain,omitempty"`
}

type LoginCookieField struct {
	ID       string                   `json:"id"`
	Required bool                     `json:"required"`
	Sources  []LoginCookieFieldSource `json:"sources"`
	Pattern  string                   `json:"pattern,omitempty"`
}

type LoginCookie struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Domain   string `json:"domain"`
	Path     string `json:"path,omitempty"`
	Secure   bool   `json:"secure,omitzero"`
	HTTPOnly bool   `json:"http_only,omitzero"`
}

type LoginCookiesParams struct {
	URL               string             `json:"url"`
	UserAgent         string             `json:"user_agent,omitempty"`
	InitialCookies    []LoginCookie      `json:"initial_cookies,omitempty"`
	Fields            []LoginCookieField `json:"fields"`
	ExtractJS         string             `json:"extract_js,omitempty"`
	WaitForURLPattern string             `json:"wait_for_url_pattern,omitempty"`
	Hidden            bool               `json:"hidden,omitzero"`
}

func (lcp *LoginCookiesParams) MarshalZerologObject(e *zerolog.Event) {
	e.Str("url", lcp.URL).
		Str("user_agent", lcp.UserAgent).
		Int("initial_cookie_count", len(lcp.InitialCookies)).
		Any("fields", lcp.Fields).
		Str("wait_for_url_pattern", lcp.WaitForURLPattern).
		Bool("hidden", lcp.Hidden)
}

type LoginInputFieldType string

const (
	LoginInputFieldTypeUsername    LoginInputFieldType = "username"
	LoginInputFieldTypePassword    LoginInputFieldType = "password"
	LoginInputFieldTypePhoneNumber LoginInputFieldType = "phone_number"
	LoginInputFieldTypeEmail       LoginInputFieldType = "email"
	LoginInputFieldType2FACode     LoginInputFieldType = "2fa_code"
	LoginInputFieldTypeToken       LoginInputFieldType = "token"
	LoginInputFieldTypeURL         LoginInputFieldType = "url"
	LoginInputFieldTypeDomain      LoginInputFieldType = "domain"
	LoginInputFieldTypeSelect      LoginInputFieldType = "select"
	LoginInputFieldTypeCaptchaCode LoginInputFieldType = "captcha_code"
)

type LoginInputDataField struct {
	Type         LoginInputFieldType          `json:"type"`
	ID           string                       `json:"id"`
	Name         string                       `json:"name"`
	Description  string                       `json:"description"`
	DefaultValue string                       `json:"default_value,omitempty"`
	Pattern      string                       `json:"pattern,omitempty"`
	MinLength    int                          `json:"min_length,omitzero"`
	MaxLength    int                          `json:"max_length,omitzero"`
	Options      []string                     `json:"options,omitempty"`
	Validate     func(string) (string, error) `json:"-"`
}

func (f *LoginInputDataField) FillDefaultValidate() {
	if f.Validate != nil {
		return
	}
	if f.Pattern != "" {
		f.Validate = func(s string) (string, error) {
			match, err := regexp.MatchString(f.Pattern, s)
			if err != nil {
				return "", err
			} else if !match {
				return "", fmt.Errorf("doesn't match regex `%s`", f.Pattern)
			}
			return strings.TrimSpace(s), nil
		}
	} else {
		f.Validate = func(s string) (string, error) { return s, nil }
	}
}

type LoginUserInputParams struct {
	Fields      []LoginInputDataField       `json:"fields"`
	Attachments []*LoginUserInputAttachment `json:"attachments"`
	CanCancel   bool                        `json:"can_cancel,omitzero"`
}

type LoginUserInputAttachment struct {
	Type     event.MessageType            `json:"type,omitempty"`
	FileName string                       `json:"filename,omitempty"`
	Content  []byte                       `json:"content,omitempty"`
	Info     LoginUserInputAttachmentInfo `json:"info,omitzero"`
}

type LoginUserInputAttachmentInfo struct {
	MimeType string `json:"mimetype,omitempty"`
	Width    int    `json:"w,omitzero"`
	Height   int    `json:"h,omitzero"`
	Size     int    `json:"size,omitzero"`
}

type LoginCompleteParams struct {
	UserLoginID string `json:"user_login_id"`
}
