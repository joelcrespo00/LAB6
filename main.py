import datetime as dt
import random
from neo4j import GraphDatabase

#Credencials DB
uri = "bolt://localhost:7687"
userName = "neo4j"
password = "test"

# DATA
flag = [1, 0]
nation = ["Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Anguilla", "Antigua &amp; Barbuda", "Argentina",
          "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados"]
address = ["Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia"]
region = ["Alaska", "Alabama", "Arkansas", "Arizona", "California"]
mfgr = ["A", "B", "C"]
tipus = ["FD", "D"]
dates = ["2020-01-01", "1900-10-10", "2024-05-01", "2021-01-01"]
segment = ["Seg1", "Seg2", "Seg3"]


def drop_and_restart(session):
    session.run("MATCH(n) DETACH DELETE n")

    create_part(session)
    create_supp(session)
    create_partsupp(session)
    create_order(session)
    create_customer(session)
    create_region(session)
    create_segment(session)
    #connect_nodes(session)


def create_part(session):
    for i in range(10):
        session.run("CREATE (n:Part {partkey: " + str(i) + ",mfgr: '" + random.choice(mfgr) +
                    "', size: " + str(random.randint(0, 10)) + ",type: '" + random.choice(tipus) + "'})")


def create_supp(session):
    for i in range(10):
        session.run("CREATE (n:Supplier {suppkey:" + str(i) + ", acctbal: " + str(random.randint(0, 10)) +
                    ", name:'" + random.choice(nation) + "', address: '" + random.choice(address) +
                    "',phone: '999999999999999', comment: 'Comentari', nationkey: " + str(i) + "})")


def create_partsupp(session):
    for i in range(10):
        session.run("CREATE (n:Partsupp {partkey:" + str(i) +
                    ", supplycost: " + str(random.randint(0, 10)) + ", suppkey: " + str(i) + "})")


def create_order(session):
    for i in range(10):
        session.run("CREATE (n:Order {orderkey:" + str(i) +
                    ", orderdate: '" + random.choice(dates) +
                    "', shippriority:" + str(random.randint(0, 10)) + ", custkey: " + str(i) + "})")


def create_customer(session):
    for i in range(10):
        session.run("CREATE (n:Customer {custkey:" + str(i) + ", nationkey: " + str(i) + "})")


def create_region(session):
    for i in range(10):
        session.run("CREATE (n:region {regionkey:" + str(i) + ", name: '" + random.choice(region) + "'})")


def create_segment(session):
    for i in range(10):
        session.run("CREATE (n:Segment {mktsegment:'" + random.choice(segment) + "'})")

def connect_nodes(session):
    session.run()

def print_menu():
    print("Que vols fer¿?:\n",
          "0 --> Data\n",
          "1 --> Q1\n",
          "2 --> Q2\n",
          "3 --> Q3\n",
          "4 --> Q4\n")


def valid_date(date):
    year, month, day = date.split('-')
    try:
        dt.datetime(int(year), int(month), int(day))
    except ValueError:
        return False
    return True


def q1(session, date):
    q1 = session.run()

    print("Q1 results:")
    for row in q1:
        print(row)


def q2(session, size, type, region):
    q2 = session.run()

    print("Q1 results:")
    for row in q2:
        print(row)


def q3(session, mktsegment, date1, date2):
    q3 = session.run()

    print("Q3 results:")
    for row in q3:
        print(row)


def q4(session, region, date):
    q4 = session.run()

    print("Q4 results:")
    for row in q4:
        print(row)


if __name__ == '__main__':

    driver = GraphDatabase.driver(uri, auth=(userName, password))
    session = driver.session()

    drop_and_restart(session)

    print_menu()

    op = int(input("Quina acció desitja realitzar?"))

    while op != -1:
        if op == 0:
            #nodes = session.run("MATCH (n)-[r]->(m) RETURN n,r,m")
            nodes = session.run("MATCH (n) RETURN n")
            for node in nodes:
                print(node)

        elif op == 1:
            # Entrar la data a comparar
            date = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date):
                date = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")

            q1(session, dt.datetime.strptime(date, "%Y-%m-%d"))

        elif op == 2:  # region, type, size
            size = input("Introdueix un valor numeric per l'atribut 'size': ")
            type = input("Introdueix un valor per l'atribut 'type' ")
            region = input("Introdueix un valor per l'atribut 'region' ")

            q2(session, float(size), str(type), str(region))

        elif op == 3:
            mkt_segment = input("Introdueix un valor per l'atribut 'mkt_segment'")
            date1 = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date1):
                date1 = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")
            date2 = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date2):
                date2 = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")

            q3(session, str(mkt_segment), dt.datetime.strptime(date1, "%Y-%m-%d"),dt.datetime.strptime(date2, "%Y-%m-%d"))

        elif op == 4:
            region = input("Introdueix un valor per l'atribut 'region'")
            date = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date):
                date = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")

            q4(session, str(region), dt.datetime.strptime(date, "%Y-%m-%d"))

        print_menu()
        op = input("Desitja fer alguna accio mes?")
        op = int(op)


