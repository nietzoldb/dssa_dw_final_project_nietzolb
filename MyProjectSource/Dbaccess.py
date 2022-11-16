from sqlalchemy import create_engine 


def p_sum(func):
    def inner(a,b):
        print("The sum " + str(a) + " + " + str(b) + " is ", end="")
        return func(a,b)
    return inner

@p_sum
def sum(a,b):
    summed = a + b
    print(summed)

if __name__ == "__main__":
    sum(2,7)

def my_decorator(msg='hi'):
    def this_is_dec(func):
        def wrapper():
            print(msg)
            print('this before call func')
            func()
            print('this after call func')
        return wrapper
    return this_is_dec

@my_decorator()
def printName():
    print("Show me")
    

printName()
            

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"
sq.create_engine()