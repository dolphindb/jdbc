n=2;

//void = [NULL, NULL];

bool = [1b, 0b];

char = [97c, 'A'];

short = [122h, 123h];

int = [21, 22];

long = [22l, 23l];

float  = [2.1f, 2.2f];

double = [2.1, 2.2];

string= [`Hello, `world];

date = [2013.06.13, 2013.06.14];

month = [2016.06M, 2016.07M];

time = [13:30:10.008, 13:30:10.009];

minute = [13:30m, 13:31m];

second = [13:30:10, 13:30:11];

datetime = [2012.06.13 13:30:10, 2012.06.13 13:30:10];

timestamp = [2012.06.13 13:30:10.008, 2012.06.13 13:30:10.009];

nanotime = [13:30:10.008007006, 13:30:10.008007007];

nanotimestamp = [2012.06.13 13:30:10.008007006, 2012.06.13 13:30:10.008007007];


//char.short,int,long,,float,double,;

t1= table(bool,char,short,int,long,float,double,string,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp);

t1;

saveTable("/home/swang/src/dolphindb-jdbc/dolphindb-jdbc/data/dballdata", t1, `t1);