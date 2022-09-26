package structs

type RestSetting struct {
	Url         string `json:"url"`
	Method      string `json:"method"`
	Body        string `json:"body"`
	ContentType string `json:"contentType"`
}
