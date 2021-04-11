from file_ingestor import Ingestor
import sys,getopt
import glob
import os.path
from datetime import datetime, timedelta
def main(argv):
    start_time = datetime.now()
    inputfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:",["ifile="])
    except getopt.GetoptError:
        print('app.py -i <inputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('app.py -i <inputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
    print('Input file is ', inputfile)
    print
    if(os.path.splitext(inputfile)[1] != '.csv'):
        raise Exception("Please import a file with .csv extension")
    else:
        try:
            ingestor_obj = Ingestor(inputfile)
        except Exception as e:
            raise e
        try:
            ingestor_obj.init_database()
        except Exception as e:
            raise e
        try:
            ingestor_obj.ingest_file()
        except Exception as e:
            raise Exception("Ingetion failed with an error", str(e))
        try:
            ingestor_obj.aggregate_data()
        except Exception as e:
            raise Exception("Aggregation failed with an error", str(e))
    print("finished in : ", datetime.now()-start_time , " seconds")
if __name__ == "__main__":
    main(sys.argv[1:])
