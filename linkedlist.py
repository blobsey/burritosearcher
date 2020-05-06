from sys import stdout

# The node within the linked list
# Each node has a value and a pointer
class LLNode:
    def __init__(self, value):
        self.value = value
        self.next = None

# The actual linked list
# A custom sort function can be used to sort nodes        
class LL:
    def __init__(self, sort=lambda a, b: a if a > b else b):
        self.head = None
        self.sort = sort
        self.length = 0
    
    def insert(self, value):
        node = LLNode(value)
        if self.head == None:
            self.head = node
            node.next = None
        elif self.sort(self.head.value, value) == self.head.value:
            node.next = self.head
            self.head = node
        else:
            current = self.head
            while (current.next != None and self.sort(current.next.value, value)):
                current = current.next
            node.next = current.next
            current.next = node
        self.length += 1
            
    def delete(self, value):
        if self.head.value == value:
            self.head = self.head.next
        else:
            current = self.head 
            while (current.next != None and current.next.value != value):
                current = current.next 
            if current != None:
                current.next = current.next.next
        self.length -= 1
        
    def __len__(self):
        return self.length
           
    def __getitem__(self, key):
        if self.head == None:
            raise IndexError("LL.__getitem__(" + str(key) + "): linked list is empty")
        if type(key) is int:
            if not -1 * self.length < key < self.length: raise IndexError("LL.__getitem__(" + str(key) + "): " + str(key) + " is out of bounds")
            ref = key if key > 0 else key + self.length
            count = 0
            current = self.head
            while (current.next != None and count != ref):
                current = current.next
                count += 1
            return current
        else:
            current = self.head
            while (current != None and current.value != key):
                current = current.next
            if current == None: raise KeyError("LL.__getitem__(" + str(key) + "): " + str(key) + " not found in linked list")
            return current

    def __setitem__(self, key, value):
        node = self.__getitem__(key)
        node.value = value

    def __str__(self):
        result = ""
        current = self.head
        while (current != None):
            result += str(current.value) + "\n"
            current = current.next
        return result