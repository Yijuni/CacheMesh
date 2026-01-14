package store

//链表节点
type Element[T any] struct {
	Value T
	prev *Element[T]
	next *Element[T]
}

//双向链表
type List[T any] struct {
	head *Element[T]
	tail *Element[T]
	len int
}

//创建空链表
func New[T any]() *List[T]{
	return &List[T]{}
}

//初始化list
func (l *List[T]) Init(){
	l.head = nil
	l.tail = nil
	l.len = 0
}

//返回链表长度
func (l *List[T])Len() int{
	return l.len
}

//在尾部添加元素,返回新元素的指针
func (l *List[T]) PushBack(value T) *Element[T]{
	element := &Element[T]{
		Value:value,
	}
	if l.tail==nil{ //第一个元素
		l.head = element
		l.tail = element
	}else{
		l.tail.next = element
		element.prev = l.tail
		l.tail = element
	}
	l.len++
	return element
}

//在头部插入元素
func (l *List[T]) PushFront(value T) *Element[T]{
	element := &Element[T]{
		Value:value,
	}
	if(l.tail==nil){
		l.head = element
		l.tail = element
	}else{
		element.next = l.head
		l.head.prev = element
		l.head = element
	}
	l.len++
	return element
}

//删除指定节点
func (l *List[T]) Remove(element *Element[T]){
	if element==nil{
		return
	}

	if element.prev!=nil{
		element.prev.next = element.next
	}else{
		l.head = element.next
	}

	if element.next!=nil{
		element.next.prev = element.prev
	}else{
		l.tail = element.prev
	}
	l.len--
}

//将某个元素移动到队尾
func (l *List[T]) MoveToBack(element *Element[T]){
	l.Remove(element)
	if l.tail==nil{
		l.head = element
		l.tail = element
	}else{
		l.tail.next = element
		element.prev = l.tail
		l.tail = element
	}
	l.len++ //Remove减少了1，移动到末尾得增加一
}

//返回队头元素
func (l *List[T]) Front() *Element[T]{
	return l.head
}

//返回队尾元素
func (l *List[T]) Back() *Element[T]{
	return l.tail
}