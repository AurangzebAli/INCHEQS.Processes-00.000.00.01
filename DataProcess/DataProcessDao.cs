using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
//using INCHEQS.Helpers;
using INCHEQS.Processes.DataProcess;
//using INCHEQS.Processes.DataProcess;
//using INCHEQS.Security.Account;
//using INCHEQS.Security;
using System.Globalization;
using INCHEQS.DataAccessLayer;
using INCHEQS.Common;
//using INCHEQS.Helpers;

public class DataProcessDao : IDataProcessDao {
    
    private readonly ApplicationDbContext dbContext;

    public DataProcessDao(ApplicationDbContext dbContext) {
        this.dbContext = dbContext;

    }

    public List<string> GetFileNameFromFileManager(string taskId) {
        DataProcessModel dataProcess = new DataProcessModel();
        List<string> resultList = new List<string>();

        string stmt = "SELECT fldFileName FROM tblFileManager WHERE fldTaskId=@fldTaskId";
        DataTable dt = dbContext.GetRecordsAsDataTable(stmt, new[] { new SqlParameter("@fldTaskId", taskId) });

        foreach (DataRow row in dt.Rows) { 
            string fileName = row["fldFileName"].ToString();
            resultList.Add(fileName);
        }
        return resultList;
    }

    public DataTable ListAll(/*AccountModel*/string bankCode) {
        DataTable ds = new DataTable();
        string stmt = "select dp.fldPrimaryId,dp.fldProcessName,dp.fldStatus, convert(varchar,dp.fldstartTime,108) as fldStartTime, convert(varchar,dp.fldEndTime,108) as fldEndTime,um.fldUserAbb,convert(varchar,dp.fldClearDate,103) as fldClearDate from tblDataProcess dp, tblUserMaster um where dp.fldUpdateUserId = um.fldUserId and um.fldBankCode=@fldBankCode";

            ds = dbContext.GetRecordsAsDataTable(stmt, new[] { new SqlParameter("@fldBankCode", bankCode/*currentUser.BankCode*/) });

        return ds;
    }


    public void DeleteProcessUsingCheckbox(string dataProcess)
    {
        string[] aryText = dataProcess.Split(',');
        if ((aryText.Length > 0))
        {
            string stmt = "delete from tblDataProcessICS where fldDataProcessId in (" + DatabaseUtils.getParameterizedStatementFromArray(aryText) + ")";

            dbContext.ExecuteNonQuery(stmt, DatabaseUtils.getSqlParametersFromArray(aryText).ToArray());

        }

    }


    public void InsertClearDate(string clearDate, int lastSequence,string bankCode) {

        string stmt = "Update tblInwardClearDate set fldActiveStatus='0' where fldBankCode=@fldBankCode";
        dbContext.ExecuteNonQuery(stmt, new[] {
            new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/)
        });

        Dictionary<string, dynamic> sqlMap = new Dictionary<string, dynamic>();
        sqlMap.Add("fldClearDateID", lastSequence);
        sqlMap.Add("fldClearDate", DateUtils.formatDateToSql(clearDate));
        sqlMap.Add("fldActiveStatus", "1");
        sqlMap.Add("fldBankCode", bankCode/*CurrentUser.Account.BankCode*/);

        dbContext.ConstructAndExecuteInsertCommand("tblInwardClearDate", sqlMap);
    }

    public void DeleteDataProcess(string processName, string posPayType,string bankCode) {

        string deleteDataSql = "DELETE FROM tbldataprocess WHERE (fldprocessname = @fldprocessname OR fldPosPayType = @fldPosPayType) and fldBankCode=@fldBankCode";

        dbContext.ExecuteNonQuery(deleteDataSql, new[] {
            new SqlParameter("@fldProcessName", processName),
            new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/),
            new SqlParameter("@fldPosPayType", posPayType)
        });

    }

    public void DeleteDataProcessWithoutPosPayType(string processName,string bankCode) {

        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        try
        {
            sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
            sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));
            this.dbContext.ExecuteNonQuery(CommandType.StoredProcedure, "spcdDataProcesswithoutSystemType", sqlParameterNext.ToArray());


        }
        catch (Exception exception)
        {
            throw exception;
        }

    }

    public void DeleteDataProcessWithoutPosPayTypeICS(string processName, string bankCode)
    {

        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        try
        {
            sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
            sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));
            this.dbContext.ExecuteNonQuery(CommandType.StoredProcedure, "spcdDataProcesswithoutSystemTypeICS", sqlParameterNext.ToArray());


        }
        catch (Exception exception)
        {
            throw exception;
        }

    }

    public void DeleteDataProcessWithPosPayType(string processName, string posPayType,string bankCode)
    {

        string deleteDataSql = "DELETE FROM tbldataprocess WHERE fldprocessname = @fldprocessname and fldPosPayType = @fldPosPayType and fldBankCode=@fldBankCode";

        dbContext.ExecuteNonQuery(deleteDataSql, new[] {
            new SqlParameter("@fldProcessName", processName),
            new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/),
            new SqlParameter("@fldPosPayType", posPayType)
        });

    }



    public void InsertToDataProcess(/*AccountModel*/string bankCode, string processName, string posPayType, string clearDate, string reUpload, string taskId, string batchId, string crtuserId, string upduserId, string filename = "")
    {



        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
        sqlParameterNext.Add(new SqlParameter("@fldSystemType", posPayType));
        sqlParameterNext.Add(new SqlParameter("@fldStatus", 1));
        sqlParameterNext.Add(new SqlParameter("@fldProductCode", "OCS"));
        sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));
        sqlParameterNext.Add(new SqlParameter("@fldProcessDate", DateUtils.formatDateToSql(clearDate)));
        sqlParameterNext.Add(new SqlParameter("@fldCreateUserId", crtuserId));
        sqlParameterNext.Add(new SqlParameter("@fldCreateTimeStamp", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldStartTime", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldEndTime", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldUpdateUserId", upduserId));
        sqlParameterNext.Add(new SqlParameter("@fldUpdateTimeStamp", DateTime.Now));

        this.dbContext.ExecuteNonQuery(CommandType.StoredProcedure, "spciDataProcessOCS", sqlParameterNext.ToArray());
    }


    public void InsertToDataProcessICS(/*AccountModel*/string bankCode, string processName, string posPayType, string clearDate, string reUpload, string taskId, string batchId , string crtuserId, string upduserId, string filename = "")
    {

       

        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
        sqlParameterNext.Add(new SqlParameter("@fldPosPayType", posPayType));//@fldSystemType
        sqlParameterNext.Add(new SqlParameter("@fldStatus", 1));
        sqlParameterNext.Add(new SqlParameter("@fldClearDate", DateUtils.formatDateToSql(clearDate)));
        sqlParameterNext.Add(new SqlParameter("@fldCreateUserId", crtuserId));
        sqlParameterNext.Add(new SqlParameter("@fldCreateTimeStamp", DateUtils.GetCurrentDatetimeForSql()));
        sqlParameterNext.Add(new SqlParameter("@fldStartTime", DateUtils.GetCurrentDatetimeForSql()));
        sqlParameterNext.Add(new SqlParameter("@fldEndTime", DateUtils.GetCurrentDatetimeForSql()));
        sqlParameterNext.Add(new SqlParameter("@fldUpdateUserId", upduserId));
        sqlParameterNext.Add(new SqlParameter("@fldUpdateTimeStamp", DateUtils.GetCurrentDatetimeForSql()));
        sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));



        //sqlParameterNext.Add(new SqlParameter("@fldProductCode", "ICS"));
        //sqlParameterNext.Add(new SqlParameter("@fldProcessDate", DateUtils.formatDateToSql(clearDate)));



        //sqlParameterNext.Add(new SqlParameter("@fldTaskId", taskId));

        this.dbContext.ExecuteNonQuery(CommandType.StoredProcedure, "spciDataProcessICS", sqlParameterNext.ToArray());
    }
    // xx start 20210610

    public void InsertSuccessToDataProcessICS(/*AccountModel*/string bankCode, string processName, string posPayType, string clearDate, string reUpload, string taskId, string batchId, string crtuserId, string upduserId, string filename = "")
    {



        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
        sqlParameterNext.Add(new SqlParameter("@fldPosPayType", posPayType));
        sqlParameterNext.Add(new SqlParameter("@fldStatus", 4));
        sqlParameterNext.Add(new SqlParameter("@fldClearDate", clearDate));
        sqlParameterNext.Add(new SqlParameter("@fldCreateUserId", crtuserId));
        sqlParameterNext.Add(new SqlParameter("@fldCreateTimeStamp", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldStartTime", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldEndTime", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldUpdateUserId", upduserId));
        sqlParameterNext.Add(new SqlParameter("@fldUpdateTimeStamp", DateTime.Now));
        sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));
        //sqlParameterNext.Add(new SqlParameter("@fldProductCode", "ICS"));
        //sqlParameterNext.Add(new SqlParameter("@fldProcessDate", DateUtils.formatDateToSql(clearDate)));
        //sqlParameterNext.Add(new SqlParameter("@fldTaskId", taskId));

        this.dbContext.ExecuteNonQuery(CommandType.StoredProcedure, "spciSuccessDataProcessICS", sqlParameterNext.ToArray());
    }

    public DataTable GetProcessStatus(string clearDate, string processName,string bankCode) {

        DataTable dt = new DataTable();
        string sql = "SELECT ISNULL(fldRemarks,'') as fldRemarks, * FROM tblDataProcessICS WHERE fldProcessName = @fldProcessName AND fldProcessDate = @fldClearDate and fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp";

        dt = dbContext.GetRecordsAsDataTable(sql, new[] {
            new SqlParameter("@fldProcessName", processName),
            new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/),
            new SqlParameter("@fldClearDate" , DateUtils.formatDateToSql(clearDate))
        });

        return dt;
    }
    // xx end 20210610
    public DataTable GetProcessStatusEOD(string clearDate, string processName,string bankCode)
    {
        string sql;
        DataTable dt = new DataTable();
        if (String.IsNullOrEmpty(clearDate))
        {
            sql = "SELECT top 1 ISNULL(fldRemarks,'') as fldRemarks, fldProcessDate as fldcleardate, * FROM tblDataProcessICS WHERE fldProcessName = 'ICSEOD' and fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp desc";
        }
        else
        {
            sql = "SELECT top 1 ISNULL(fldRemarks,'') as fldRemarks, fldProcessDate as fldcleardate, * FROM tblDataProcessICS WHERE fldProcessName = @fldProcessName /*AND fldClearDate = @fldClearDate*/ and fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp desc";
        }
        dt = dbContext.GetRecordsAsDataTable(sql, new[] {
            new SqlParameter("@fldProcessName", processName),
            new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/)
        });

        return dt;
    }

    public DataTable GetProcessStatusICL(string clearDate, string posPayType, string bankCode, string processName) {

        DataTable dt = new DataTable();
        string sql = "SELECT ISNULL(fldRemarks,'') as fldRemarks, * FROM tblDataProcess WHERE fldPosPayType = @posPayType AND fldClearDate = @fldClearDate and fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp";
        
        dt = dbContext.GetRecordsAsDataTable( sql, new[] {
            new SqlParameter("@posPayType" , posPayType),
            new SqlParameter("@fldBankCode" , bankCode/*CurrentUser.Account.BankCode*/),
            new SqlParameter("@fldClearDate" , DateUtils.formatDateToSql(clearDate))
        });
        
        return dt;
    }

    public DataTable GetProcessStatusECCS(string filetype, string clearDate, string processName,string bankCode) {

        DataTable dt = new DataTable();
        string sql = "SELECT ISNULL(fldRemarks,'') as fldRemarks, * FROM tblDataProcess WHERE fldProcessName = @fldProcessName AND fldClearDate = @fldClearDate AND RIGHT(RTRIM(fldPosPayType),12) = @filetype and fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp";

        dt = dbContext.GetRecordsAsDataTable(sql, new[] {
            new SqlParameter("@fldProcessName", processName),
            new SqlParameter("@filetype", filetype),
            new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/),
            new SqlParameter("@fldClearDate" , DateUtils.formatDateToSql(clearDate))
        });

        return dt;
    }

    public DataTable GetClearingType(string clearingType) {
        DataTable dt = new DataTable();
        string sql = "SELECT fldClearingNumber as clearingValue, fldClearingDesc as clearingText FROM tblClearingType with (NOLOCK) WHERE fldType =@fldType";
        dt = dbContext.GetRecordsAsDataTable( sql, new[] { new SqlParameter("@fldType", clearingType) });
        
        return dt;
    }

    public bool CheckRunningProcess(string processName, string posPayType, string clearDate,string bankCode) {
        DataTable dt = new DataTable();
        string result = "";
        string extract = "";
        string sql = "";
        if (processName == "ICSImport")
        {
            extract = "ICSExtractICL";
            sql = "SELECT Top 1 isnull(fldRemarks,'') as fldRemarks, fldStatus FROM tblDataProcess WHERE (fldProcessName = @fldProcessName or fldProcessName = @fldProcessName2) AND fldCleardate = @fldCleardate AND fldPosPayType = @fldPosPayType AND fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp DESC ";
        }
        else
        {
            sql = "SELECT Top 1 isnull(fldRemarks,'') as fldRemarks, fldStatus FROM tblDataProcess WHERE fldProcessName = @fldProcessName AND fldCleardate = @fldCleardate AND fldPosPayType = @fldPosPayType AND fldBankCode=@fldBankCode ORDER BY fldCreateTimestamp DESC ";
        }
        
        List<SqlParameter> parameters = new List<SqlParameter>();
        parameters.Add(new SqlParameter("@fldProcessName", processName));
        parameters.Add(new SqlParameter("@fldProcessName2", extract));
        parameters.Add(new SqlParameter("@fldCleardate", DateUtils.formatDateToSql(clearDate)));
        parameters.Add(new SqlParameter("@fldPosPayType", posPayType));
        parameters.Add(new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/));
        dt = dbContext.GetRecordsAsDataTable( sql, parameters.ToArray());
        
        if (dt.Rows.Count > 0) {
            result = dt.Rows[0]["fldStatus"].ToString() ;
        }

        if(result.Equals("")||result.Equals("4")) { //Result "" for initialize and "4" for complete
            return true;
        }
        return false;
    }

    public bool CheckRunningProcessWithoutPosPayType(string processName, string clearDate,string bankCode) {
        DataTable dt = new DataTable();
        string result = "";
        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
        sqlParameterNext.Add(new SqlParameter("@fldProcessDate", clearDate));
        sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));

        dt = dbContext.GetRecordsAsDataTableSP("spcgRunningProcessWithoutSystemType", sqlParameterNext.ToArray());
        if (dt.Rows.Count > 0)
        {
            result = dt.Rows[0]["fldStatus"].ToString();
        }

        if (result.Equals("") || result.Equals("4")) { //Result "" for initialize and "4" for complete
            return true;
        }
        return false;
    }

    public bool CheckRunningProcessWithoutPosPayTypeICS(string processName, string clearDate, string bankCode)
    {
        DataTable dt = new DataTable();
        string result = "";
        List<SqlParameter> sqlParameterNext = new List<SqlParameter>();
        sqlParameterNext.Add(new SqlParameter("@fldProcessName", processName));
        sqlParameterNext.Add(new SqlParameter("@fldProcessDate", clearDate));
        sqlParameterNext.Add(new SqlParameter("@fldBankCode", bankCode));

        dt = dbContext.GetRecordsAsDataTableSP("spcgRunningProcessWithoutSystemTypeICS", sqlParameterNext.ToArray());
        if (dt.Rows.Count > 0)
        {
            result = dt.Rows[0]["fldStatus"].ToString();
        }

        if (result.Equals("") || result.Equals("4"))
        { //Result "" for initialize and "4" for complete
            return true;
        }
        return false;
    }

    public bool CheckRunningProcessBeforeEod(string processName, string clearDate,string bankCode)
    {
        DataTable dt = new DataTable();
        string result = "";
        string sql = "";
        sql = string.Format("SELECT fldStatus FROM tblDataProcess WHERE fldProcessName in ({0})  /*AND fldCleardate = @fldCleardate*/ AND fldBankCode=@fldBankCode and fldstatus<>'4' ", processName);

        //sql = string.Format("SELECT fldStatus FROM tblDataProcessICS WHERE fldProcessName in ({0})  /*AND fldCleardate = @fldCleardate*/ AND fldBankCode=@fldBankCode and fldstatus<>'4' ", processName); 

        List<SqlParameter> parameters = new List<SqlParameter>();
        parameters.Add(new SqlParameter("@fldCleardate", DateUtils.formatDateToSql(clearDate)));
        parameters.Add(new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/));
        dt = dbContext.GetRecordsAsDataTable(sql, parameters.ToArray());

        if (dt.Rows.Count > 0)
        {
            result = dt.Rows[0]["fldStatus"].ToString();
        }

        if (result.Equals("") || result.Equals("4"))
        { //Result "" for initialize and "4" for complete
            return true;
        }
        return false;
    }

    public DataTable GenGif(string processName, string clearDate,string bankCode)
    {
        DataTable dt = new DataTable();
        string result = "";
        string sql = "";
        sql = string.Format("SELECT fldUIC,fldimagefolder FROM view_items WHERE fldcleardate = @fldCleardate and fldissuebankcode=@fldBankCode ");

        List <SqlParameter> parameters = new List<SqlParameter>();
        parameters.Add(new SqlParameter("@fldCleardate", DateUtils.formatDateToSql(clearDate)));
        parameters.Add(new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/));
        dt = dbContext.GetRecordsAsDataTable(sql, parameters.ToArray());

        if (dt.Rows.Count > 0)
        {
            return dt;
        }
        else
        {
            return null;
        }
       
    }

    public bool CheckRunningProcessGenerateFile(string processName, string posPayType, string clearDate,string bankCode)
    {
        DataTable dt = new DataTable();
        string result = "";
        string sql = "SELECT fldStatus FROM tblDataProcess WHERE fldProcessName = @fldProcessName AND fldCleardate = @fldCleardate AND fldPosPayType = @fldPosPayType AND fldBankCode=@fldBankCode ";

        List<SqlParameter> parameters = new List<SqlParameter>();
        parameters.Add(new SqlParameter("@fldProcessName", processName));
        parameters.Add(new SqlParameter("@fldCleardate", DateUtils.formatDateToSql(clearDate)));
        parameters.Add(new SqlParameter("@fldPosPayType", posPayType));
        parameters.Add(new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/));
        dt = dbContext.GetRecordsAsDataTable(sql, parameters.ToArray());

        if (dt.Rows.Count > 0)
        {
            result = dt.Rows[0]["fldStatus"].ToString();
        }

        if (result.Equals("") || result.Equals("4"))
        { //Result "" for initialize and "4" for complete
            return true;
        }
        return false;
    }
    public bool CheckProcessDateWithinRetentionPeriod(string sProcessingDate, int sProcess,string bankCode)
    {
        bool sContinue = false;
        string stmt = "Select * from tblAuditLogRetention";
        int typeDayNumber = 0;
        int result;
        string archiveLog = "";
        string clearingDate = "";
        string stmt2 = "Select top 1 fldcleardate from tblinwarditeminfo where fldissuebankcode = @fldBankCode order by fldcleardate desc";
        List<SqlParameter> parameters = new List<SqlParameter>();
        parameters.Add(new SqlParameter("@fldBankCode", bankCode/*CurrentUser.Account.BankCode*/));
        DataTable ds = new DataTable();
        ds = dbContext.GetRecordsAsDataTable(stmt);

        if (ds.Rows.Count > 0)
        {
            DataRow row = ds.Rows[0];
            archiveLog = row["fldAchAuditLog"].ToString();
        }

        
        DataTable configTable = dbContext.GetRecordsAsDataTable(stmt2, parameters.ToArray());
        if (configTable.Rows.Count > 0)
        {
            clearingDate = configTable.Rows[0]["fldcleardate"].ToString();
        }
        else
        {
            clearingDate = DateTime.Today.ToString();
        }

        DateTime sProcessDate = DateTime.ParseExact(sProcessingDate, "dd-MM-yyyy", CultureInfo.InvariantCulture);
        DateTime todayDate = Convert.ToDateTime(clearingDate);

        if (sProcess == 0)
        {
            if (todayDate != sProcessDate)
            {
                sContinue = false;
            }
            else
            {
                sContinue = true;
            }
        }
        
        if (sProcess == 1) { 
            string type = archiveLog.Substring(archiveLog.Length-1);
            string num = archiveLog.Substring(0, archiveLog.Length - 1);
            //Mid(datahousekeep.archiveLog, 1, tblRes.Rows(0)("fldAuditLog").ToString.Length - 1)
            bool isNumeric = int.TryParse(num, out result);

            //DateTime sProcessDate = DateTime.Parse(sProcessingDate);
           

            if (isNumeric == false)
            {
                sContinue = false;
            }

            switch (type)
            {
                case "D":
                    typeDayNumber = 1;
                    break;
                case "M":
                    typeDayNumber = 30;
                    break;
                case "Y":
                    typeDayNumber = 365;
                    break;
            }
            int periorNumber = Int32.Parse(num);
            int rententionDay =0;
            rententionDay = typeDayNumber * periorNumber;
            double diff = (todayDate - sProcessDate).TotalDays;

                if (diff > rententionDay)
                {
                    sContinue = false;
                }
                else
                {
                    sContinue = true;
                }

        }
        return sContinue;
    }


}