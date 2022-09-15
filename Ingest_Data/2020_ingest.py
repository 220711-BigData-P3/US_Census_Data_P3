
from pyspark.sql import SparkSession,context
import wget as wg
import os as oss
import zipfile as zp
import shutil
import upload_s3

spark=SparkSession.builder\
    .master("local")\
    .appName("myRDD")\
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")
sc=spark.sparkContext

MyDir='/home/jed/'
MyFile='al2020.pl.zip'
MyFilePath=MyDir+MyFile
My_Out_Put_P1='2020_P1.csv'
My_Out_Put_P2='2020_P2.csv'
My_Out_Put_GEO='2020_GEO.csv'
#df=spark.read.csv(MyFilePath,sep='|',header=False)
#df.show(10)
My_Gloabl_Header_P1='FILEID,STUSAB,CHARITER,CIFSN,LOGRECNO,P0010001,P0010002,P0010003,P0010004,P0010005,P0010006,\
P0010007,P0010008,P0010009,P0010010,P0010011,P0010012,P0010013,P0010014,P0010015,P0010016,P0010017,P0010018,\
P0010019,P0010020,P0010021,P0010022,P0010023,P0010024,P0010025,P0010026,P0010027,P0010028,P0010029,P0010030,\
P0010031,P0010032,P0010033,P0010034,P0010035,P0010036,P0010037,P0010038,P0010039,P0010040,P0010041,P0010042,\
P0010043,P0010044,P0010045,P0010046,P0010047,P0010048,P0010049,P0010050,P0010051,P0010052,P0010053,P0010054,\
P0010055,P0010056,P0010057,P0010058,P0010059,P0010060,P0010061,P0010062,P0010063,P0010064,P0010065,P0010066,\
P0010067,P0010068,P0010069,P0010070,P0010071,P0020001,P0020002,P0020003,P0020004,P0020005,P0020006,P0020007,\
P0020008,P0020009,P0020010,P0020011,P0020012,P0020013,P0020014,P0020015,P0020016,P0020017,P0020018,P0020019,\
P0020020,P0020021,P0020022,P0020023,P0020024,P0020025,P0020026,P0020027,P0020028,P0020029,P0020030,P0020031,\
P0020032,P0020033,P0020034,P0020035,P0020036,P0020037,P0020038,P0020039,P0020040,P0020041,P0020042,P0020043,\
P0020044,P0020045,P0020046,P0020047,P0020048,P0020049,P0020050,P0020051,P0020052,P0020053,P0020054,P0020055,\
P0020056,P0020057,P0020058,P0020059,P0020060,P0020061,P0020062,P0020063,P0020064,P0020065,P0020066,P0020067,\
P0020068,P0020069,P0020070,P0020071,P0020072,P0020073\n'


My_Gloabl_Header_P2='FILEID,STUSAB,CHARITER,CIFSN,LOGRECNO,P0030001,P0030002,P0030003,P0030004,P0030005, \
P0030006,P0030007,P0030008,P0030009,P0030010,P0030011,P0030012,P0030013,P0030014,P0030015,P0030016,P0030017,\
P0030018,P0030019,P0030020,P0030021,P0030022,P0030023,P0030024,P0030025,P0030026,P0030027,P0030028,P0030029, \
P0030030,P0030031,P0030032,P0030033,P0030034,P0030035,P0030036,P0030037,P0030038,P0030039,P0030040,P0030041,\
P0030042,P0030043,P0030044,P0030045,P0030046,P0030047,P0030048,P0030049,P0030050,P0030051,P0030052,P0030053,\
P0030054,P0030055,P0030056,P0030057,P0030058,P0030059,P0030060,P0030061,P0030062,P0030063,P0030064,P0030065, \
P0030066,P0030067,P0030068,P0030069,P0030070,P0030071,P0040001,P0040002,P0040003,P0040004,P0040005,P0040006,\
P0040007,P0040008,P0040009,P0040010,P0040011,P0040012,P0040013,P0040014,P0040015,P0040016,P0040017,P0040018, \
P0040019,P0040020,P0040021,P0040022,P0040023,P0040024,P0040025,P0040026,P0040027,P0040028,P0040029,P0040030, \
P0040031,P0040032,P0040033,P0040034,P0040035,P0040036,P0040037,P0040038,P0040039,P0040040,P0040041,P0040042,\
P0040043,P0040044,P0040045,P0040046,P0040047,P0040048,P0040049,P0040050,P0040051,P0040052,P0040053,P0040054,\
P0040055,P0040056,P0040057,P0040058,P0040059,P0040060,P0040061,P0040062,P0040063,P0040064,P0040065,P0040066, \
P0040067,P0040068,P0040069,P0040070,P0040071,P0040072,P0040073,H0010001,H0010002,H0010003\n'

My_Gloabl_Header_GEO='FILEID,STUSAB,SUMLEV,GEOVAR,GEOCOMP,CHARITER,CIFSN,LOGRECNO,GEOID,GEOCODE,REGION,\
IVISION,STATE,STATENS,COUNTY,COUNTYCC,COUNTYNS,COUSUB,COUSUBCC,COUSUBNS,SUBMCD,SUBMCDCC,SUBMCDNS,ESTATE,\
ESTATECC,ESTATENS,CONCIT,CONCITCC,CONCITNS,PLACE,PLACECC,PLACENS,TRACT,BLKGRP,BLOCK,AIANHH,AIHHTLI,AIANHHFP,\
AIANHHCC,AIANHHNS,AITS,AITSFP,AITSCC,AITSNS,TTRACT,TBLKGRP,ANRC,ANRCCC,ANRCNS,CBSA,MEMI,CSA,METDIV,NECTA,\
NMEMI,CNECTA,NECTADIV,CBSAPCI,NECTAPCI,UA,UATYPE,UR,CD116,CD118,CD119,CD120,CD121,SLDU18,SLDU22,SLDU24,\
SLDU26,SLDU28,SLDL18,SLDL22,SLDL24,SLDL26,SLDL28,VTD,VTDI,ZCTA,SDELM,SDSEC,SDUNI,PUMA,AREALAND,AREAWATR,\
BASENAME,NAME,FUNCSTAT,GCUNI,POP100,HU100,INTPTLAT,INTPTLON,LSADC,PARTFLAG,UGA\n'

def get_sensus_Data(Inval):

######################################################################################################################################
#
#Spencer'simplementation
#
######################################################################################################################################
#https://www2.census.gov/census_2000/datasets/Summary_File_3/
#https://www2.census.gov/census_2000/datasets/redistricting_file_--p1_94-171/
    oss.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/'+Inval)
    


def Extract_My_Data(filepath):
    with zp.ZipFile(filepath,'r')as zip_ref:
        NameList=zip_ref.namelist()
        zip_ref.extractall()#toextractallfiles
        zip_ref.close()
    return NameList


def Detete_Files(inval):
    P1=str(inval)+'000012020.pl'
    P2=str(inval)+'000022020.pl'
    P3=str(inval)+'000032020.pl'
    P4=str(inval)+'geo2020.pl'
    P5=str(inval)+'2020.pl.zip'

    if oss.path.exists(P1):
        oss.remove(P1)

    if oss.path.exists(P2):
        oss.remove(P2)

    if oss.path.exists(P3):
        oss.remove(P3)

    if oss.path.exists(P4):
        oss.remove(P4)

    if oss.path.exists(P5):
        oss.remove(P5)


def extract_info_1(inval,MyGlobalString):
    fnm=inval
    fnm+='000012020.pl'
    with open(fnm,'r',encoding='latin1')as f:
        read=f.readline()
        read=read.replace('|',',')
        MyGlobalString += read
        return MyGlobalString


def extract_info_2(inval,MyGlobalString):
    fnm=inval
    fnm+='000022020.pl'
    with open(fnm,'r',encoding='latin1')as f:
        read=f.readline()
        read=read.replace('|',',')
        MyGlobalString += read
        return MyGlobalString


def extract_info_GEO(inval,MyGlobalString):
    fnm=inval
    fnm+='geo2020.pl'
    with open(fnm,'r', encoding='latin1')as f:
        read=f.readline()
        read=read.replace('|',',')
        MyGlobalString += read
        return MyGlobalString        


def Write_My_File(input_string, Output_file_Name):
    with open(Output_file_Name,'w')as w:
        w.write(input_string)
    
    

def main():
    MyGlobalString_1=My_Gloabl_Header_P1
    MyGlobalString_2=My_Gloabl_Header_P2
    MyGlobalString_GEO=My_Gloabl_Header_GEO
    MyStates=['Alabama/',
    'Alaska/',
    'Arizona/',
    'Arkansas/',
    'California/',
    'Colorado/',
    'Connecticut/',
    'Delaware/',
    'District_of_Columbia/',
    'Florida/',
    'Georgia/',
    'Hawaii/',
    'Idaho/',
    'Illinois/',
    'Indiana/',
    'Iowa/',
    'Kansas/',
    'Kentucky/',
    'Louisiana/',
    'Maine/',
    'Maryland/',
    'Massachusetts/',
    'Michigan/',
    'Minnesota/',
    'Mississippi/',
    'Missouri/',
    'Montana/',
    'Nebraska/',
    'Nevada/',
    'New_Hampshire/',
    'New_Jersey/',
    'New_Mexico/',
    'New_York/',
    'North_Carolina/',
    'North_Dakota/',
    'Ohio/',
    'Oklahoma/',
    'Oregon/',
    'Pennsylvania/',
    'Puerto_Rico/',
    'Rhode_Island/',
    'South_Carolina/',
    'South_Dakota/',
    'Tennessee/',
    'Texas/',
    'Utah/',
    'Vermont/',
    'Virginia/',
    'Washington/',
    'West_Virginia/',
    'Wisconsin/',
    'Wyoming/'
    ]

    states_abv=['al','ak','az','ar','ca','co','ct','de','dc','fl','ga','hi','id','il','in','ia','ks','ky','la','me',\
    'md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj','nm','ny',\
    'nc','nd','oh','ok','or','pa','pr','ri','sc','sd','tn','tx','ut','vt','va','wa','wv','wi','wy']

    for i in range(0,len(MyStates)):
        print(f'Extraing {MyStates[i]}......')
        # if i==17:
        #     break
        #print (">>>",MyStates[i])
        get_sensus_Data(MyStates[i])
        pathconstruct=str(states_abv[i])+'2020.pl.zip'
        Extract_My_Data(pathconstruct)
        MyGlobalString_1=extract_info_1(states_abv[i],MyGlobalString_1)
        MyGlobalString_2=extract_info_2(states_abv[i],MyGlobalString_2)
        MyGlobalString_GEO=extract_info_GEO(states_abv[i],MyGlobalString_GEO)
        Detete_Files(states_abv[i])
        print ("i: ", i , " ")
        Write_My_File(MyGlobalString_1,My_Out_Put_P1)
        Write_My_File(MyGlobalString_2,My_Out_Put_P2)
        Write_My_File(MyGlobalString_GEO,My_Out_Put_GEO)

    #Upload Generated Files into S3 Bucket
    upload_s3.upload_file_s3(My_Out_Put_P1)
    upload_s3.upload_file_s3(My_Out_Put_P2)
    upload_s3.upload_file_s3(My_Out_Put_GEO)
    
    #Remove Generated Files from Local FS
    oss.remove(My_Out_Put_P1)
    oss.remove(My_Out_Put_P2)
    oss.remove(My_Out_Put_GEO)


    print('extractioncompleted')
if __name__=='__main__':
    main()