


# DATA
flag = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
nation = ["Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Anguilla", "Antigua &amp; Barbuda", "Argentina",
          "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados",
          "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia"]
region = ["Alaska",
          "Alabama",
          "Arkansas",
          "Arizona",
          "California"]


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


if __name__ == '__main__':
    client = MongoClient("mongodb://sergi:sergi@127.0.0.1:27017/cbde-lab5")
    db = client["cbde-lab5"]

    create_and_generate_partsupp_collection(db)
    create_order_collection(db)

    print_menu()

    op = int(input("Quina acció desitja realitzar?"))

    while op != -1:
        if op == 0:
            print("Collection Order")
            for doc in db["order"].find():
                print(doc)

            print("Collection Partsupp")
            for doc in db["partsupp"].find():
                print(doc)

        elif op == 1:
            # Entrar la data a comparar
            date = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date):
                date = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")
            q1(db["order"], dt.datetime.strptime(date, "%Y-%m-%d"))

        elif op == 2:  # region, type, size
            size = input("Introdueix un valor numeric per l'atribut 'size': ")
            type = input("Introdueix un valor per l'atribut 'type' ")
            region = input("Introdueix un valor per l'atribut 'region' ")
            q2(db["partsupp"], float(size), str(type), str(region))

        elif op == 3:
            mkt_segment = input("Introdueix un valor per l'atribut 'mkt_segment'")
            date1 = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date1):
                date1 = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")
            date2 = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date2):
                date2 = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")
            q3(db["order"], str(mkt_segment), dt.datetime.strptime(date1, "%Y-%m-%d"),
               dt.datetime.strptime(date2, "%Y-%m-%d"))

        elif op == 4:
            region = input("Introdueix un valor per l'atribut 'region'")
            date = input("Introdueix data amb format següent: YYYY-mm-dd ")
            while not valid_date(date):
                date = input("Format incorrecte. Introdueix data amb format següent: YYYY-mm-dd ")
            q4(db["order"], str(region), dt.datetime.strptime(date, "%Y-%m-%d"))

        print_menu()
        op = input("Desitja fer alguna accio mes?")
        op = int(op)


##### REQUIREMENTS
#pymongo==3.12.1
#names==0.3.0
#faker