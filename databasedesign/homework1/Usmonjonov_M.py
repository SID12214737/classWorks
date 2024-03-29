########################### DO NOT MODIFY THIS SECTION ##########################
#################################################################################
import sqlite3
from sqlite3 import Error
import csv
#################################################################################

######################################################################

class A1_sql():
    ############### DO NOT MODIFY THIS SECTION ###########################
    ######################################################################
    def create_connection(self, path):
        connection = None
        try:
            connection = sqlite3.connect(path)
            connection.text_factory = str
        except Error as e:
            print("Error occurred: " + str(e))
    
        return connection

    def execute_query(self, connection, query):
        cursor = connection.cursor()
        try:
            if query == "":
                return "Query Blank"
            else:
                cursor.execute(query)
                connection.commit()
                return "Query executed successfully"
        except Error as e:
            return "Error occurred: " + str(e)
        
    def import_data(self,connection,path):
        with open(path, encoding="utf8") as f:
            reader = csv.reader(f)
            movielist = list(reader)
            for movie in movielist:
                #print(movie)
                connection.execute("INSERT INTO movies VALUES (?,?,?)",(movie[0],movie[1], movie[2]))

        sql = "SELECT COUNT(id) FROM movies;"
        cursor = connection.execute(sql)
        return cursor.fetchall()[0][0]
    
    def import_data2(self,connection, path):
        ############### CREATE IMPORT CODE BELOW ############################
        with open(path, encoding="utf8") as f:
            reader = csv.reader(f)
            cast_list = list(reader)
            for cast in cast_list:
                #print(movie)
                connection.execute("INSERT INTO movie_cast VALUES (?,?,?,?,?)",(cast[0],cast[1], cast[2], cast[3], cast[4]))
        ######################################################################
        
        sql = "SELECT COUNT(cast_id) FROM movie_cast;"
        cursor = connection.execute(sql)
        return cursor.fetchall2()[0][0]
    ######################################################################
    ######################################################################

    # Part a InhaID [1 point]
    def InhaID(self):
        return "12214737"
    
    # Part b createTable_1 [2 points]
    def createTable_1(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_b_sql = "create table `movies` (id int primary key, title text, score real)"
        ######################################################################
        
        return self.execute_query(connection, part_b_sql)

    # Part c createTable_2 [2 points]
    def createTable_2(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_c_sql = "create table `movie_cast` (movie_id int, cast_id int, cast_name text, brthday text, popularity real)"
        ######################################################################
        
        return self.execute_query(connection, part_c_sql)       

    # Part d_1 createIndex_1 [1 points]
    def createIndex_1(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_d_1_sql = "create index movie_index on movies (id)"
        ######################################################################
        return self.execute_query(connection, part_d_1_sql)
    
    # Part d_2 createIndex_2 [1 points]
    def createIndex_2(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_d_2_sql = "create index cast_index on movie_cast (cast_id)"
        ######################################################################
        return self.execute_query(connection, part_d_2_sql)
    
    # Part e calcProportion [3 points]
    def calcProportion(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_e_sql = "select round(cast(count(*) as real) / (select count(*) from movies) * 100, 2) from movies where score between 7 and 20"
        ######################################################################
        cursor = connection.execute(part_e_sql)
        return cursor.fetchall()[0][0]

    # Part f prolificActors [5 points]
    def prolificActors(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_f_sql = "select cast_name, count(*) as appearance_count from movie_cast where popularity>10 group by cast_name order by appearance_count desc, cast_name asc limit 5"
        ######################################################################
        cursor = connection.execute(part_f_sql)
        return cursor.fetchall()

    # Part g highScoringMovie [5 points]
    def highScoringMovie(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_g_sql = "select title as movie_title, round(score, 2) as score, count(*) as cast_count from movies join movie_cast on movies.id = movie_cast.movie_id group by movies.id order by score desc, cast_count asc, movie_title asc limit 5"
        ######################################################################
        cursor = connection.execute(part_g_sql)
        return cursor.fetchall()
    
    # Part h highScoringActor [5 points]
    def highScoringActor(self,connection):
        ############### EDIT SQL STATEMENT ###################################
        part_h_sql = "select cast_id, cast_name, round(avg(movies.score), 2) as average_score from movie_cast join movies on movie_cast.movie_id = movies.id where movie_id in ( select movie_id from movie_cast join movies ON movie_cast.movie_id = movies.id where movies.score >= 25 group by movie_id having avg(movies.score) >= 25 ) group by cast_id having count(*) >= 3 order by average_score DESC, cast_name asc limit 10"
        ######################################################################
        cursor = connection.execute(part_h_sql)
        return cursor.fetchall()


if __name__ == "__main__":
    
    ########################### DO NOT MODIFY THIS SECTION ##########################
    #################################################################################
    db = A1_sql()
    try:
        conn = db.create_connection("Q2")
    except:
        print("Database Creation Error")

    try:
        conn.execute("DROP TABLE IF EXISTS movies;")
        conn.execute("DROP TABLE IF EXISTS movie_cast;")
        conn.execute("DROP TABLE IF EXISTS cast_bio;")
        conn.execute("DROP VIEW IF EXISTS good_collaboration;")
        conn.execute("DROP TABLE IF EXISTS movie_overview;")
    except Exception as e:
        print("Error in Table Drops")
        print(e)

    try:
        stu_id = db.InhaID()
        if stu_id=="Write your ID here":
            print("Error in Part a")
        else:
            print('\033[32m' + "part b: " + '\033[m' + stu_id)
    except Exception as e:
        print("Error in Part a")
        print(e)
    
    try:
        print('\033[32m' + "part b: " + '\033[m' + str(db.createTable_1(conn)))
    except Exception as e:
        print("Error in Part b")
        print(e)

    try:
        print('\033[32m' + "part c: " + '\033[m' + str(db.createTable_2(conn)))
    except Exception as e:
        print("Error in Part c")
        print(e)

    try:
        print('\033[32m' + "Row count for Movies Table: " + '\033[m' + str(db.import_data(conn,"data/movies.csv")))
        print('\033[32m' + "Row count for Movie Cast Table: " + '\033[m' + str(db.import_data2(conn,"data/movie_cast.csv")))
    except Exception as e:
        print("Error in importing data")
        print(e)

    try:
        print('\033[32m' + "part d 1: " + '\033[m' + db.createIndex_1(conn))
        print('\033[32m' + "part d 2: " + '\033[m' + db.createIndex_2(conn))
    except Exception as e:
        print("Error in part d")
        print(e)

    try:
        print('\033[32m' + "part e: " + '\033[m' + str(db.calcProportion(conn)))
    except Exception as e:
        print("Error in part e")
        print(e)

    try:
        print('\033[32m' + "part f: " + '\033[m')
        for line in db.prolificActors(conn):
            print(line[0],line[1])
    except Exception as e:
        print("Error in part f")
        print(e)

    try:
        print('\033[32m' + "part g: " + '\033[m')
        for line in db.highScoringMovie(conn):
            print(line[0],line[1],line[2])
    except Exception as e:
        print("Error in part g")
        print(e)

    try:
        print('\033[32m' + "part h: " + '\033[m')
        for line in db.highScoringActor(conn):
            print(line[0],line[1],line[2])
    except Exception as e:
        print("Error in part h")
        print(e)

    conn.close()
    #################################################################################
    #################################################################################
  
