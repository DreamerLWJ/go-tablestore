package api

type Api interface {
	WriteData() error
}

type SearchOption struct {
}
