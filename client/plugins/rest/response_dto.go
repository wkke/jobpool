package rest

type ResponseDto struct {
	Code  int64  `json:",code"`
	Error string `json:",error"`
}
