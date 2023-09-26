using INCHEQS.Processes.DataProcess;
//using INCHEQS.Security.Account;
using System.Collections.Generic;
using System.Data;

namespace INCHEQS.Processes.DataProcess {
    public interface IDataProcessDao {
        DataTable ListAll(/*AccountModel*/string bankCode);

        DataTable GetProcessStatus(string clearDate, string posPayType,string bankCode);
        DataTable GetProcessStatusEOD(string clearDate, string posPayType,string bankCode);
        DataTable GetProcessStatusICL(string clearDate, string posPayTypes,string bankCode,string processName);
        DataTable GetProcessStatusECCS(string filetype, string clearDate, string posPayType,string bankCode);
        DataTable GenGif(string processName, string clearingDate,string bankCode);

        DataTable GetClearingType(string clearingType);
        List<string> GetFileNameFromFileManager(string taskId);

        void DeleteProcessUsingCheckbox(string processName);
        bool CheckRunningProcess(string processName, string posPayType, string clearingDate,string bankCode);
        bool CheckRunningProcessWithoutPosPayType(string processName, string clearingDate,string bankCode);
        bool CheckRunningProcessWithoutPosPayTypeICS(string processName, string clearingDate, string bankCode);
        bool CheckRunningProcessBeforeEod(string processName, string clearingDate,string bankCode);
        void InsertClearDate(string clearDate, int lastSequence,string bankCode);
        void DeleteDataProcess(string processName, string posPayType,string bankCode);
        void DeleteDataProcessWithoutPosPayType(string processName,string bankCode);
        void DeleteDataProcessWithoutPosPayTypeICS(string processName, string bankCode);
        void DeleteDataProcessWithPosPayType(string processName, string posPayType,string bankCode);
        void InsertToDataProcess(/*AccountModel*/string bankCode, string processName, string posPayType, string clearingDate, string reUpload , string taskId, string batchId,string crtuserId,string upduserId, string fileName = "");
        void InsertToDataProcessICS(/*AccountModel*/string bankCode, string processName, string posPayType, string clearingDate, string reUpload, string taskId, string batchId, string crtuserId, string upduserId, string fileName = "");
        bool CheckProcessDateWithinRetentionPeriod(string clearingDate,int sProcess,string bankCode);
        void InsertSuccessToDataProcessICS(/*AccountModel*/string bankCode, string processName, string posPayType, string clearDate, string reUpload, string taskId, string batchId, string crtuserId, string upduserId, string filename = "");
    }
}