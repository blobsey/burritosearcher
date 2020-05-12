# Author: Nichole

# Component of the Linked List
# Each node must have a value and a next pointer
class LLNode:
    def __init__(self, value):
        self.value = value
        self.next = None

# Linked List Implementation
# By default, the linked list uses a sort function similar to writing "if a < b".
# However, since there are different pieces of information you can store
# in postings and some pieces of information might require specialized 
# sorting functions, there is an option to pass in a custom function as an argument
class LL:
    def __init__(self, contents=None, sort=lambda a, b: a if a > b else b):
        self.head = None
        self.sort = sort
        self.length = 0
        if contents != None: {self.insert(c) for c in contents}
    
    # Insert a new node into the linked list 
    # If the linked list is empty, insert the new node and readjust the head pointer
    # If the linked list has one entry, insert the new node before or after
    # the current head node based on the result of the sort function.
    # Else, find the linked node's new position
    #
    # Update self.length so that we don't have to traverse the linked list
    # every time we want to get the length.
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
            while (current.next != None and self.sort(current.next.value, value) != current.next.value):
                current = current.next
            node.next = current.next
            current.next = node
        self.length += 1
     
    # Delete a node from the list
    # If the node doesn't exist, a ValueError gets raised.
    # Otherwise, we delete the node and adjust pointers.
    def delete(self, value):
        if self.head == None: raise ValueError("LL.delete(" + str(value) + "): linked list is empty")
        if self.head.value == value:
            self.head = self.head.next
            self.length -= 1
        else:
            current = self.head 
            while (current.next != None and current.next.value != value):
                current = current.next 
            if current != None:
                current.next = current.next.next
                self.length -= 1
            else:
                raise ValueError("LL.delete(" + str(value) + "): " + str(value) + " does not exist in linked list")
        
    def intersect(self, ll2: "linked list", valsort=lambda a, b: a if a < b else b, valequal=lambda a, b: a==b) -> "list":    
        answer = set()
        curr1 = self.head
        curr2 = ll2.head
        while curr1 != None and curr2 != None:
            if valequal(curr1.value, curr2.value):
                answer.add(curr1.value)
                curr1 = curr1.next
                curr2 = curr2.next
            elif valsort(curr1.value, curr2.value) == curr1.value:
                curr1 = curr1.next
            else:
                curr2 = curr2.next
        return answer
        
    # Perform an intersection between 2 linked list
    def merge(self, ll2: "linked list", valsort=lambda a, b: a if a < b else b, valequal=lambda a, b: a==b) -> "list":
        answer = set()
        if self == ll2:
            curr1 = self.head
            while curr1 != None:
                answer.add(curr1.value)
                curr1 = curr1.next
        else: answer = self.intersect(ll2, valsort, valequal)        
        return answer
        
    # Perform the intersection between 2 list using the '&' operator   
    def __and__(self, ll2, valsort=lambda a, b: a if a < b else b, valequal=lambda a, b:a==b):
        return self.merge(ll2, valsort, valequal)

    # Check if two linked lists are equal to each other
    # Assume that the lists are sorted
    def __eq__(self, ll2):
        if self.head.value != ll2.head.value: return False
        curr1 = self.head
        curr2 = ll2.head
        while curr1 != None and curr2 != None:
            if curr1.value != curr2.value:
                return False
            curr1 = curr1.next
            curr2 = curr2.next
        return True
        
    # Get the length of the linked list
    # To avoid traversing the entire linked list, we return a variable
    # with the length stored inside
    def __len__(self):
        return self.length
           
    # Retrieves an item from the linked list.
    # Implemented for convenience
    # The index can either by an int or another object
    # The index can be positive or negative. Invalid indexes raise IndexErrors.
    # Int index has priority. Calling L[0] on a linked list of 1 -> 0 will return 1.
    # If the index passed in is not an int, the function will try
    # to retrieve an object whose value matches the key. 
    # This is done because it's possible for the postings to store information
    # that is not an int (such as tuples representing proximity matches or 
    # docIDs).
    # If no value can be found, KeyError gets raised
    def __getitem__(self, key):
        if self.head == None:
            raise IndexError("LL.__getitem__(" + str(key) + "): linked list is empty")
        if type(key) is int:
            if not -1 * self.length < key < self.length: raise IndexError("LL.__getitem__(" + str(key) + "): " + str(key) + " is out of bounds")
            ref = key if key >= 0 else key + self.length
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
    
    # Sets the value of a node to a new value
    # If the key isn't, a KeyError or IndexError will be raised.
    # A new node doesn't get created because the list is sorted, and the
    # node may not end up in the key position. And creating filler nodes
    # to make up for large indexes takes up more space, violating the
    # goal of reducing space requirements.
    def __setitem__(self, key, value):
        node = self.__getitem__(key)
        node.value = value

    # Returns a string representation of the linked list
    # Meant to help with debugging
    def __str__(self, sep=" -> "):
        result = ""
        current = self.head
        while (current != None):
            result += str(current.value) + sep
            current = current.next
        return result[:-1 * len(sep)]