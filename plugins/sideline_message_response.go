package plugins

type SidelineMessageResponse struct {
	Success                     bool
	ConcurrentModificationError bool
	UnknownError                bool
	ErrorMessage                string
}
