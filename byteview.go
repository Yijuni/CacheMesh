package cachemesh
//缓存视图，主要为了防止外部修改缓存值
type ByteView struct {
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}

func (b ByteView) ByteSlice() []byte {
	return cloneByte(b.b)
}

func (b ByteView) String() string{
	return string(b.b)
}

func cloneByte(b []byte) []byte{
	tmp := make([]byte,len(b))
	copy(tmp,b)
	return tmp
}