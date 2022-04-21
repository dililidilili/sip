/*
gcc odbc_multi_thread.c -o test  -lthread -lodbc
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlext.h>

SQLHENV henv;
SQLHDBC hdbc;
int ConnectNum = 0;

static int DumpODBCLog(SQLHENV hEnv, SQLHDBC hDbc, SQLHSTMT hStmt)
{
    SQLCHAR     szError[501];
    SQLCHAR     szSqlState[10];
    SQLINTEGER  nNativeError;
    SQLSMALLINT nErrorMsg;

	int rec;
	if ( hStmt )
	{
		rec = 0;
		while ( SQLGetDiagRec( SQL_HANDLE_STMT, hStmt, ++rec, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
		{
			printf( "[%s]%s\n", szSqlState, szError );
		}
	}

	if ( hDbc )
	{
		rec = 0;
		while ( SQLGetDiagRec( SQL_HANDLE_DBC, hDbc, ++rec, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
		{
			printf( "[%s]%s\n", szSqlState, szError );
		}
	}

	if ( hEnv )
	{
		rec = 0;
		while ( SQLGetDiagRec( SQL_HANDLE_ENV, hEnv, ++rec, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
		{
			printf( "[%s]%s\n", szSqlState, szError );
		}
	}
  
    return 1;
}




int DBConnect(SQLHSTMT *hstmt)
{
	SQLRETURN ret;

	if (ConnectNum == 0) 
	{
		//数据库连接
		ret = SQLConnect(hdbc, (SQLCHAR*)"OracleODBC-12c", SQL_NTS, NULL, SQL_NTS, NULL, SQL_NTS);
		if (!SQL_SUCCEEDED(ret)) //第二个参数是之前配置的数据源，后面是数据库用户名和密码
		{
			DumpODBCLog(0, hdbc, 0);
			return -1;
		}
	}
	//分配执行语句句柄
	if (SQLAllocHandle(SQL_HANDLE_STMT, hdbc, hstmt) != SQL_SUCCESS)
	{
		
		DumpODBCLog(0, hdbc, *hstmt);
		return -1;
	}	
	ConnectNum++;
	printf("DBDisConnect\n");	

	return 0;
}


int DBDisConnect(SQLHSTMT *hstmt)
{
	if (*hstmt) {
		SQLFreeHandle(SQL_HANDLE_STMT, *hstmt);
		*hstmt = NULL;
	}

	if (ConnectNum > 0)
	{
		ConnectNum --;
	}
	if (ConnectNum == 0)
	{
		SQLDisconnect(hdbc);
	}
	printf("DBDisConnect\n");	
	return 0;
}


int DBExec(SQLCHAR *sql, SQLHSTMT *hstmt)
{
	SQLRETURN ret;
	ret = SQLPrepare(*hstmt, (SQLCHAR*)sql, strlen((char *)sql));
	if (ret != SQL_SUCCESS)
	{
	   DumpODBCLog(henv, hdbc, *hstmt);		   
	   return -1;
	}

	ret = SQLExecute(*hstmt);
    if (ret == SQL_NO_DATA)
    {
 		printf("SQL_NO_DATA\n");
   }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
 		printf("SQL_SUCCESS_WITH_INFO\n");
    }
    else if (ret != SQL_SUCCESS)
    {
        DumpODBCLog(henv, hdbc, *hstmt);
        return -1;
    }
	return 0;
}

void odbc_thread(void *handle)
{
	SQLHSTMT 					hstmt;
	int							ret;
	SQLLEN						nIndicator = 0;
	
	char *QueryStr = "select BindType ,BindIp from UserTbl where Username='admin' and Status=1";

	while(1)
	{
		ret = DBConnect(&hstmt);
		if (ret) {
			printf("ERROR: Could not DBConnect\n");
		}

		//SvrTbl:
		//snprintf(QueryStr, DB_QUERY_STR_MAX_LEN, "select PublicID, IP, NatIP, SipPort from SvrTbl where Type=200");

		//执行
		ret = DBExec((SQLCHAR *)QueryStr, &hstmt);
		if (ret) {
			DBDisConnect(&hstmt);
			printf("ERROR: DBExec\n");
		}
		
		DBDisConnect(&hstmt);
		sleep(1);
	}
	return;
}
int main(int argc, char **argv)
{  
    SQLRETURN ret;
	
 
    //分配环境句柄
	if(SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv) != SQL_SUCCESS)
	{
		printf("ERROR: Could not SQLAllocHandle( SQL_HANDLE_ENV )\n");
		DumpODBCLog(henv, hdbc, 0);
		return -1;
	}
 
    //设置环境属性
	if (SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)3, 0) != SQL_SUCCESS)
	{
		printf("ERROR: Could not SQLSetEnvAttr( SQL_HANDLE_DBC )\n");
		DumpODBCLog(henv, 0, 0);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return -1;
	}
	
    //分配连接句柄
	if (SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc) != SQL_SUCCESS)
	{
		printf("ERROR: Could not SQLAllocHandle( SQL_HANDLE_DBC )\n");
		DumpODBCLog(henv, hdbc, 0);
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
		return -1;
	}


	#define thread_num  1
	int t = 0;
	int thread[thread_num] = {0};
	for( t = 0; t < thread_num; t++)
	{
		printf("Creating thread %d\n", t);
		ret = pthread_create(&thread[t], NULL, odbc_thread, NULL);
		if (ret)
		{
			printf("ERROR; return code is %d\n", ret);
			return -1;
		}
	}
	
   sleep(6000);
   
   
	//断开数据库连接
    ret = SQLDisconnect(hdbc);
    if(SQL_SUCCESS != ret && SQL_SUCCESS_WITH_INFO != ret) {
        printf("disconnected error!");
     	DumpODBCLog(0, hdbc, 0);             
	}
	//释放连接句柄
    ret = SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    if(SQL_SUCCESS != ret && SQL_SUCCESS_WITH_INFO != ret) {
        printf("free hdbc error!");
     	DumpODBCLog(0, hdbc, 0);             
	}
	//释放环境句柄
    ret=SQLFreeHandle(SQL_HANDLE_ENV, henv);
    if(SQL_SUCCESS != ret && SQL_SUCCESS_WITH_INFO != ret) {
        printf("free henv error!");
     	DumpODBCLog(henv, 0, 0);             
	}
	return 0;
}