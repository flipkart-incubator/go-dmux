package sideline

type SidelineMessageResponse struct {
	Success                     bool
	ConcurrentModificationError bool
	UnknownError                bool
	ErrorMessage                string
}
