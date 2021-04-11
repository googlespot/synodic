package org.galaxy.synodic.affine.collection;

import net.lecousin.framework.collections.ArrayUtil;

import java.util.*;

/**
 *
 *
 */
public class LinkedArrayList<T> implements List<T> {

	public LinkedArrayList(int arraySize) {
		this.arraySize = arraySize;
	}

	private int      arraySize;
	private Array<T> head = null;
	private Array<T> tail = null;
	private long     size = 0;

	private static class Array<T> {
		@SuppressWarnings("unchecked")
		public Array(int size, Array<T> previous, Array<T> next) {
			elements = (T[]) new Object[size];
			this.previous = previous;
			this.next = next;
			if (previous != null) previous.next = this;
			if (next != null) next.previous = this;
		}

		private T[]      elements;
		private int      size = 0;
		private Array<T> previous;
		private Array<T> next;

		private void add(T o) {
			elements[size++] = o;
		}

		private int indexOf(Object o) {
			if (o == null) {
				for (int i = 0; i < size; ++i)
					if (elements[i] == null) return i;
				return -1;
			}
			for (int i = 0; i < size; ++i)
				if (o.equals(elements[i])) return i;
			return -1;
		}

		private int lastIndexOf(Object o) {
			if (o == null) {
				for (int i = size - 1; i >= 0; --i)
					if (elements[i] == null) return i;
				return -1;
			}
			for (int i = size - 1; i >= 0; --i)
				if (o.equals(elements[i])) return i;
			return -1;
		}
	}

	private void doubleSize() {
		Array<T> a = head;
		do {
			@SuppressWarnings("unchecked") T[] na = (T[]) new Object[arraySize * 2];
			System.arraycopy(a.elements, 0, na, 0, a.size);
			a.elements = na;
			if (a.next != null) {
				System.arraycopy(a.next.elements, 0, a.elements, a.size, a.next.size);
				a.size += a.next.size;
				a.next = a.next.next;
				if (a.next != null) a.next.previous = a;
				else tail = a;
				a = a.next;
			} else {
				break;
			}
		} while (a != null);
		arraySize *= 2;
	}

	@Override
	public boolean add(T o) {
		if (tail == null) {
			head = tail = new Array<>(arraySize, null, null);
		} else if (tail.size == arraySize) {
			tail = new Array<>(arraySize, tail, null);
		}
		tail.add(o);
		size++;
		if (size > arraySize * 100) doubleSize();
		return true;
	}

	@Override
	public void add(int index, T element) {
		if (head == null) {
			add(element);
			return;
		}
		if (size + 1 > arraySize * 100) doubleSize();
		if (index >= size) {
			add(element, tail, tail.size, false);
			return;
		}
		Array<T> a = head;
		int i = 0;
		while (i + a.size <= index && a.next != null) {
			i += a.size;
			a = a.next;
		}
		add(element, a, index - i, false);
	}

	private void add(T element, Array<T> array, int index, boolean shifting) {
		if (index > array.size) index = array.size;
		if (index == array.size) {
			if (index == arraySize) {
				if (array.next == null) {
					tail = new Array<>(arraySize, array, null);
					tail.add(element);
					if (!shifting) size++;
					return;
				}
				throw new IllegalStateException("Unexpected situation");
			}
			array.elements[index] = element;
			array.size++;
			if (!shifting) size++;
			return;
		}
		if (array.size == arraySize) {
			if (array.next == null) {
				tail = new Array<>(arraySize, array, null);
				tail.add(array.elements[arraySize - 1]);
			} else {
				add(array.elements[arraySize - 1], array.next, 0, true);
			}
			array.size--;
		}
		System.arraycopy(array.elements, index, array.elements, index + 1, array.size - index);

		array.elements[index] = element;
		array.size++;
		if (!shifting) size++;
	}

	public void addlong(long index, T element) {
		if (head == null) {
			add(element);
			return;
		}
		if (size + 1 > arraySize * 100) doubleSize();
		if (index >= size) {
			add(element, tail, tail.size, false);
			return;
		}
		Array<T> a = head;
		long i = 0;
		while (i + a.size <= index && a.next != null) {
			i += a.size;
			a = a.next;
		}
		add(element, a, (int) (index - i), false);
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		for (Iterator<? extends T> it = c.iterator(); it.hasNext(); )
			add(it.next());
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		for (Iterator<? extends T> it = c.iterator(); it.hasNext(); ++index)
			add(index, it.next());
		return true;
	}

	@Override
	public void clear() {
		head = tail = null;
		size = 0;
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o) >= 0;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Iterator<?> it = c.iterator(); it.hasNext(); )
			if (!contains(it.next())) return false;
		return true;
	}

	@Override
	public T get(int index) {
		return getlong(index);
	}

	public T getlong(long index) {
		if (head == null) throw new IndexOutOfBoundsException(Long.toString(index));
		if (index < 0) throw new IndexOutOfBoundsException(Long.toString(index));
		Array<T> a = head;
		long i = 0;
		while (i + a.size <= index && a.next != null) {
			i += a.size;
			a = a.next;
		}
		i = index - i;
		if (i >= a.size) throw new IndexOutOfBoundsException(Long.toString(index));
		return a.elements[(int) i];
	}

	@Override
	public int indexOf(Object o) {
		int index = 0;
		for (Array<T> a = head; a != null; index += a.size, a = a.next) {
			int i = a.indexOf(o);
			if (i >= 0) return i + index;
		}
		return -1;
	}

	@Override
	public boolean isEmpty() {
		return head == null;
	}

	private void shiftLeft(Array<T> a, int i) {
		if (i >= a.size) return;
		if (a.size == 1) {
			if (head == a) {
				if (tail == a) tail = null;
				else head.next.previous = null;
				head = head.next;
			} else if (tail == a) {
				tail.previous.next = null;
				tail = tail.previous;
			} else {
				a.previous.next = a.next;
				a.next.previous = a.previous;
			}
			return;
		}
		if (i < a.size - 1) System.arraycopy(a.elements, i + 1, a.elements, i, a.size - i - 1);
		a.elements[a.size - 1] = null;
		a.size--;
	}

	@Override
	public T remove(int index) {
		return removelong(index);
	}

	@Override
	public boolean remove(Object o) {
		int i = indexOf(o);
		if (i == -1) return false;
		remove(i);
		return true;
	}

	public T removelong(long index) {
		if (head == null) throw new IndexOutOfBoundsException(Long.toString(index));
		if (index < 0) throw new IndexOutOfBoundsException(Long.toString(index));
		Array<T> a = head;
		long i = 0;
		while (i + a.size <= index && a.next != null) {
			i += a.size;
			a = a.next;
		}
		i = index - i;
		if (i >= a.size) throw new IndexOutOfBoundsException(Long.toString(index));
		T element = a.elements[(int) i];
		shiftLeft(a, (int) i);
		size--;
		return element;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean changed = false;
		for (Iterator<?> it = c.iterator(); it.hasNext(); )
			changed |= remove(it.next());
		return changed;
	}

	public T removeLast() {
		if (tail == null) return null;
		T element = tail.elements[--tail.size];
		if (--size == 0) {
			head = tail = null;
			return element;
		}
		if (tail.size == 0) {
			tail.previous.next = null;
			tail = tail.previous;
		}
		return element;
	}

	public T[] removeFirstArray(Class<T> cl) {
		if (size == 0) return ArrayUtil.createGenericArrayOf(0, cl);
		T[] a;
		if (head.size == head.elements.length) {
			a = head.elements;
		} else {
			a = ArrayUtil.createGenericArrayOf(head.size, cl);
			System.arraycopy(head.elements, 0, a, 0, head.size);
		}
		if (head == tail) {
			head = tail = null;
			size = 0;
			return a;
		}
		size -= head.size;
		head.next.previous = null;
		head = head.next;
		return a;
	}

	public void insertFirstArray(T[] array, int offset, int length) {
		if (length == 0) return;
		if (length > arraySize) {
			insertFirstArray(array, offset + arraySize, length - arraySize);
			insertFirstArray(array, offset, arraySize);
			return;
		}
		Array<T> a = new Array<>(arraySize, null, head);
		System.arraycopy(array, offset, a.elements, 0, length);
		a.size = length;
		head = a;
		size += length;
	}

	public void appendArray(T[] array, int offset, int length) {
		if (length == 0) return;
		if (size + length > arraySize * 100) doubleSize();
		if (tail != null) {
			// first we fill the tail
			while (tail.size < arraySize && length > 0) {
				tail.add(array[offset++]);
				length--;
				size++;
			}
			if (length == 0) return;
		}
		do {
			Array<T> a = new Array<>(arraySize, tail, null);
			tail = a;
			int len = length > arraySize ? arraySize : length;
			System.arraycopy(array, offset, a.elements, 0, len);
			a.size = len;
			size += len;
			offset += len;
			length -= len;
		} while (length > 0);
	}

	@Override
	public int size() {
		return (int) size;
	}

	public long longsize() {
		return size;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean changed = false;
		for (ListIterator<T> it = listIterator(); it.hasNext(); ) {
			T e = it.next();
			if (!c.contains(e)) {
				it.remove();
				changed = true;
			}
		}
		return changed;
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		return new LIterator(index);
	}

	@Override
	public ListIterator<T> listIterator() {
		return listIterator(0);
	}

	@Override
	public Iterator<T> iterator() {
		return listIterator();
	}

	private class LIterator implements ListIterator<T> {
		LIterator(int index) {
			ptrNext = head;
			posNext = 0;
			nextIndex = 0;
			while (nextIndex < index && ptrNext != null) {
				if (index - nextIndex < ptrNext.size) {
					posNext = index - nextIndex;
					break;
				}
				nextIndex += ptrNext.size;
				ptrNext = ptrNext.next;
			}
		}

		private Array<T> ptrNext;
		private int      posNext;
		private int      nextIndex;

		@Override
		public boolean hasNext() {
			if (head == null || ptrNext == null) return false;
			return ptrNext != tail || posNext < ptrNext.size;
		}

		@Override
		public boolean hasPrevious() {
			if (head == null) return false;
			if (ptrNext == null) return true;
			return ptrNext != head || posNext > 0;
		}

		@Override
		public T next() {
			if (ptrNext == null || posNext == ptrNext.size) throw new NoSuchElementException();
			T e = ptrNext.elements[posNext++];
			while (posNext >= ptrNext.size) {
				ptrNext = ptrNext.next;
				if (ptrNext == null) break;
				posNext = 0;
			}
			nextIndex++;
			return e;
		}

		@Override
		public int nextIndex() {
			return nextIndex;
		}

		@Override
		public T previous() {
			if (ptrNext == null) {
				ptrNext = tail;
				posNext = ptrNext.size;
			}
			T e;
			if (posNext > 0) {
				e = ptrNext.elements[--posNext];
			} else {
				ptrNext = ptrNext.previous;
				posNext = ptrNext.size - 1;
				e = ptrNext.elements[posNext];
			}
			nextIndex--;
			return e;
		}

		@Override
		public int previousIndex() {
			return nextIndex - 1;
		}

		@Override
		public void add(T o) {
			LinkedArrayList.this.add(nextIndex, o);
			// go to next one
			posNext++;
			while (posNext >= ptrNext.size) {
				ptrNext = ptrNext.next;
				if (ptrNext == null) break;
				posNext = 0;
			}
			nextIndex++;
		}

		@Override
		public void remove() {
			LinkedArrayList.this.remove(--nextIndex);
			if (posNext > 0) posNext--;
		}

		@Override
		public void set(T o) {
			if (posNext > 0) {
				ptrNext.elements[posNext - 1] = o;
			} else {
				ptrNext.previous.elements[arraySize - 1] = o;
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T2> T2[] toArray(T2[] a) {
		if (a.length < size()) a = (T2[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size());
		int i = 0;
		for (Array<T> array = head; array != null; array = array.next) {
			System.arraycopy(array.elements, 0, a, i, array.size);
			i += array.size;
		}
		return a;
	}

	@Override
	public Object[] toArray() {
		Object[] a = new Object[size()];
		int i = 0;
		for (Array<T> array = head; array != null; array = array.next) {
			System.arraycopy(array.elements, 0, a, i, array.size);
			i += array.size;
		}
		return a;
	}

	@Override
	public int lastIndexOf(Object o) {
		int i = size();
		for (Array<T> a = tail; a != null; a = a.previous) {
			int index = a.lastIndexOf(o);
			if (index >= 0) return i - a.size + index;
			i -= a.size;
		}
		return -1;
	}

	@Override
	public T set(int index, T element) {
		if (head == null) throw new IndexOutOfBoundsException(Integer.toString(index));
		if (index < 0) throw new IndexOutOfBoundsException(Integer.toString(index));
		Array<T> a = head;
		int i = 0;
		while (i + a.size <= index && a.next != null) {
			i += a.size;
			a = a.next;
		}
		i = index - i;
		if (i >= a.size) throw new IndexOutOfBoundsException(Integer.toString(index));
		T old = a.elements[i];
		a.elements[i] = element;
		return old;
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		List<T> result = new ArrayList<>(toIndex - fromIndex);
		for (int i = fromIndex; i < toIndex; ++i)
			result.add(get(i));
		return result;
	}
}
