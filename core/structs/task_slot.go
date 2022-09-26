package structs

type JobSlot struct {
	ID          string
	Name        string
	Type        string
	CreateIndex uint64 `json:"-"`
}
