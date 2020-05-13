from lookup import lookup
from unpickler import unpickle

def run_query():
    while True:
        print("Input query search: ")
        query = input()
        #Tokenize + stem (also lowercase)
        # token_amount = len(tokens)
        # for token in query:
        #     unpickle(token.charAt(0))

        #Find documents
        #clear the dump file (has all unpickled information!)


if __name__ == "__main__":
    run_query()