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
    session.run("MATCH(n) DETACH DELETE n ")

    create_part(session)
    create_supp(session)
    create_partsupp(session)
    create_order(session)
    create_customer(session)
    create_region(session)
    create_segment(session)
    connect_nodes(session)


def create_part(session):
    for i in range(10):
        session.run("CREATE (n:Part {partkey: " + str(i) + ",mfgr: '" + random.choice(mfgr) +
                    "', size: " + str(float(random.randint(0, 10)/5)) + ",type: '" + random.choice(tipus) + "'})")


def create_supp(session):
    for i in range(10):
        session.run("CREATE (n:Supplier {suppkey:" + str(i) + ", acctbal: " + str(random.randint(0, 10)) +
                    ", name:'" + random.choice(nation) + "', address: '" + random.choice(address) +
                    "',phone: '999999999999999', comment: 'Comentari', nationkey: " + str(i) + "})")


def create_partsupp(session):
    for i in range(10):
        session.run("CREATE (n:Partsupp {partkey:" + str(i) +
                    ", supplycost: " + str(random.randint(1, 10)) + ", suppkey: " + str(i) + "})")


def create_order(session):
    for i in range(10):
        session.run("CREATE (n:Order {orderkey:" + str(i) +
                    ", orderdate: '" + random.choice(dates) +
                    "', shippriority:" + str(random.randint(0, 10)) + ", custkey: " + str(i) + "})")

    session.run("CREATE INDEX index_orderdate IF NOT EXISTS FOR (n:Order) ON(n.orderdate)")


def create_customer(session):
    for i in range(10):
        session.run("CREATE (n:Customer {custkey:" + str(i) + ", nationkey: " + str(i) +
                    ", mktsegment: '" +random.choice(segment)+"'})")


def create_region(session):
    for i in range(10):
        session.run("CREATE (n:Region {regionkey:" + str(i) + ", name: '" + random.choice(region) + "'})")

    session.run("CREATE INDEX index_rname IF NOT EXISTS FOR (n:Region) ON(n.name)")


def create_segment(session):
    for i in range(len(segment)):
        session.run("CREATE (n:Segment {mktsegment:'" + segment[i] + "'})")

def connect_nodes(session):
    for i in range(10): #ASSOCIEM PART AMB PART_SUPP
        session.run("MATCH (part"+str(i)+":Part {partkey:"+str(i)+"}), (ps"+str(i)+":Partsupp {partkey:"+str(i)+"})"
                    "CREATE ((part"+str(i)+") - [:P_PS] -> (ps"+str(i)+"))")

    for i in range(10): #ASSOCIEM SUPPLIER AMB PART_SUPP
        session.run("MATCH (supp"+str(i)+":Supplier {suppkey:"+str(i)+"}), (ps"+str(i)+":Partsupp {suppkey:"+str(i)+"})"
                    "CREATE ((supp"+str(i)+") - [:S_PS] -> (ps"+str(i)+"))")

    for i in range(20): #ASSOCIEM ORDER AMB PART_SUPP
        ps_key = str(random.randint(0, 10))
        session.run("MATCH (order:Order{orderkey:"+str(i)+"}), (ps:Partsupp{suppkey:"+str(ps_key)+", partkey:"+str(ps_key)+"})"
                    "CREATE ((order) - "
                    "[:LINEITEM {returnflag:"+str(random.randint(0,1))+"," \
                                "linestatus:"+str(random.randint(0,1))+"," \
                                "quantity:"+str(random.randint(0,20))+"," \
                                "extendedprice:"+str(random.randint(0,20))+"," \
                                "discount:" + str(random.randint(0, 20)) + "," \
                                "tax:" + str(random.randint(0, 20)) + "," \
                                "shipdate:'" + random.choice(dates) +"',"\
                                "orderkey:" + str(i) + ","\
                                "suppkey:ps.suppkey," \
                                "partkey:ps.suppkey }] -> (ps))")

    session.run("CREATE INDEX index_shipdate IF NOT EXISTS FOR ()-[l:LINEITEM]-() ON(l.shipdate)")

    for i in range(10): #ASSOCIEM ORDER AMB CUSTOMER
        c_key = str(random.randint(0, 10))
        session.run("MATCH (order:Order {orderkey:"+str(i)+"}), (customer:Customer {custkey:"+str(c_key)+"})"
                    "CREATE ((order) - [:O_C] -> (customer))")

    for i in range(10): #ASSOCIEM CUSTOMER AMB SEGMENT
        session.run("MATCH (customer:Customer {custkey:"+str(i)+"}), (segment:Segment{mktsegment: customer.mktsegment})"
                    "CREATE ((customer) - [:O_C] -> (segment))")

    for i in range(10): #ASSOCIEM CUSTOMER AMB REGION
        regionkey = random.randint(0, 10)
        session.run("MATCH (customer:Customer {custkey:"+str(i)+"}), (region:Region{regionkey:"+str(regionkey)+"})"
                    "CREATE ((customer) - [:C_NATION {"
                                 "name:'" + random.choice(nation)+ "',"\
                                "nationkey:customer.nationkey," \
                                "regionkey:"+str(regionkey)+" }] -> (region))")

    for i in range(10): #ASSOCIEM SUPPLIER AMB REGION
        regionkey = random.randint(0, 10)
        session.run("MATCH (supplier:Supplier {suppkey:"+str(i)+"}), (region:Region{regionkey:"+str(regionkey)+"})"
                    "CREATE ((supplier) - [:S_NATION {"
                                 "name:'" + random.choice(nation)+ "',"\
                                "nationkey:supplier.nationkey," \
                                "regionkey:"+str(regionkey)+" }] -> (region))")

def q1(session, date):
    q1 = session.run("MATCH (:Order)-[l:LINEITEM]-(:Partsupp) "
                     "WHERE l.shipdate <= $date "
                     "RETURN l.returnflag, l.linestatus, sum(l.quantity) AS sum_qty,"
                    "sum(l.extendedprice) AS sum_base_price,"
                    "sum(l.extendedprice*(1-l.discount)) as sum_disc_price,"
                    "sum(l.extendedprice*(1-l.discount)*(1+l.tax)) as sum_charge, avg(l.quantity) as avg_qty,"
                    "avg(l.extendedprice) as avg_price, avg(l.discount) as avg_disc, count(*) as count_order "
                     "ORDER BY l.returnflag, l.linestatus",
                     {"date": date})

    print("Q1 results:")
    for row in q1:
        print(row)


def q2(session, size, type, region):

    q2 = session.run("MATCH (ps:Partsupp)--(s:Supplier)-[n:S_NATION]-(r:Region{name:$region})  "
                     "WITH min(ps.supplycost) AS minim "
                     
                    "MATCH (p:Part{size:$size})--(ps:Partsupp{supplycost:minim})--(s:Supplier)-[n:S_NATION]-(r:Region{name:$region})  "
                     "WHERE p.type =~ $type "
                     "RETURN s.acctbal,"
                     " s.name, n.name, "
                     "p.partkey, p.mfgr, "
                     "s.address, s.phone, s.comment "
                     "ORDER BY s.acctbal desc, n.name, s.name, p.partkey",
                     {"size": size,
                      "type": type,
                      "region": region})

    print("Q2 results:")
    for row in q2:
        print(row)


def q3(session, mktsegment, date1, date2):
    q3 = session.run("MATCH (s:Segment{mktsegment:$mktsegment})--(c:Customer)--(o:Order)-[l:LINEITEM]-(:Partsupp) "
                     "WHERE o.orderdate < $date1 AND l.shipdate > $date2 "
                     "RETURN l.orderkey, "
                     "sum(l.extendedprice*(1-l.discount)) as revenue, "
                     "o.orderdate, "
                     "o.shippriority "
                     "ORDER BY revenue desc, o.orderdate",
                     {"mktsegment": mktsegment,
                      "date1":date1,
                      "date2":date2})

    print("Q3 results:")
    for row in q3:
        print(row)


def q4(session, region, date):
    date_year2 = date.year + 1
    date2 = date.replace(date_year2)

    q4 = session.run("MATCH (:Customer)--(o:Order)-[l:LINEITEM]-(:Partsupp)--(s:Supplier)-[n:S_NATION]-(r:Region{name:$region}) "
                     "WHERE o.orderdate>=$date AND o.orderdate < $date2 "
                     "RETURN n.name, "
                     "sum(l.extendedprice*(1-l.discount)) as revenue "
                     "ORDER BY revenue desc ",
                     {"region": region,
                      "date":date.__str__(),
                      "date2":date2.__str__()})

    print("Q4 results:")
    for row in q4:
        print(row)


def print_menu():
    print("Que vols fer¿?:\n",
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


if __name__ == '__main__':

    driver = GraphDatabase.driver(uri, auth=(userName, password))
    session = driver.session()

    drop_and_restart(session)

    print_menu()

    op = int(input("Quina acció desitja realitzar?"))

    while op != -1:

        if op == 1:
            # Entrar la data a comparar
            date = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date):
                date = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")

            q1(session, dt.datetime.strptime(date, "%Y-%m-%d").__str__())

        elif op == 2:  # region, type, size
            size = input("Introdueix un valor numeric per l'atribut 'size': ")
            type = input("Introdueix un valor per l'atribut 'type' ")
            region = input("Introdueix un valor per l'atribut 'region' ")

            q2(session, float(size), str(type), str(region))

        elif op == 3:
            mkt_segment = input("Introdueix un valor per l'atribut 'mkt_segment'")
            date1 = input("Introdueix data1 amb format següent: YYYY-mm-dd ")
            while not valid_date(date1):
                date1 = input("Format incorrecte. Introdueix data1 amb format següent: YYYY-mm-dd ")
            date2 = input("Introdueix data2 amb format següent: YYYY-mm-dd ")
            while not valid_date(date2):
                date2 = input("Format incorrecte. Introdueix data2 amb format següent: YYYY-mm-dd ")

            q3(session, str(mkt_segment), dt.datetime.strptime(date1, "%Y-%m-%d").__str__(),dt.datetime.strptime(date2, "%Y-%m-%d").__str__())

        elif op == 4:
            region = input("Introdueix un valor per l'atribut 'region'")
            date = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date):
                date = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")

            q4(session, str(region), dt.datetime.strptime(date, "%Y-%m-%d"))

        print_menu()
        op = input("Desitja fer alguna accio mes?")
        op = int(op)

