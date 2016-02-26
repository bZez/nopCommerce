using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Web;
using Nop.Core;
using Nop.Core.Data;
using Nop.Core.Domain.Common;
using Nop.Data;

namespace Nop.Services.Common
{
    /// <summary>
    ///  Maintenance service
    /// </summary>
    public partial class MaintenanceService : IMaintenanceService
    {
        #region Fields

        private readonly IDataProvider _dataProvider;
        private readonly IDbContext _dbContext;
        private readonly CommonSettings _commonSettings;
        #endregion

        #region Ctor

        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="dataProvider">Data provider</param>
        /// <param name="dbContext">Database Context</param>
        /// <param name="commonSettings">Common settings</param>
        public MaintenanceService(IDataProvider dataProvider, IDbContext dbContext,
            CommonSettings commonSettings)
        {
            this._dataProvider = dataProvider;
            this._dbContext = dbContext;
            this._commonSettings = commonSettings;
        }

        #endregion

        #region Utilities

        private string GetBackupsPath
        {
            get
            {
                return string.Format("{0}Administration\\backups\\", HttpContext.Current.Request.PhysicalApplicationPath);
            }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Get the current ident value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <returns>Integer ident; null if cannot get the result</returns>
        public virtual int? GetTableIdent<T>() where T: BaseEntity
        {
            if (_commonSettings.UseStoredProceduresIfSupported && _dataProvider.StoredProceduredSupported)
            {
                //stored procedures are enabled and supported by the database
                var tableName = _dbContext.GetTableName<T>();
                var result = _dbContext.SqlQuery<decimal>(string.Format("SELECT IDENT_CURRENT('[{0}]')", tableName));
                return Convert.ToInt32(result.FirstOrDefault());
            }
            
            //stored procedures aren't supported
            return null;
        }

        /// <summary>
        /// Set table ident (is supported)
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="ident">Ident value</param>
        public virtual void SetTableIdent<T>(int ident) where T : BaseEntity
        {
            if (_commonSettings.UseStoredProceduresIfSupported && _dataProvider.StoredProceduredSupported)
            {
                //stored procedures are enabled and supported by the database.

                var currentIdent = GetTableIdent<T>();
                if (currentIdent.HasValue && ident > currentIdent.Value)
                {
                    var tableName = _dbContext.GetTableName<T>();
                    _dbContext.ExecuteSqlCommand(string.Format("DBCC CHECKIDENT([{0}], RESEED, {1})", tableName, ident));
                }
            }
            else
            {
                throw new Exception("Stored procedures are not supported by your database");
            }
        }

        /// <summary>
        /// Gets all backup files
        /// </summary>
        /// <returns>Backup file collection</returns>
        public virtual IList<FileInfo> GetAllBackupFiles
        {
            get
            {
                var path = GetBackupsPath;

                if (!System.IO.Directory.Exists(path))
                {
                   System.IO.Directory.CreateDirectory(path);
                }

                var di = new DirectoryInfo(path);
                var securityRules = di.GetAccessControl();
                securityRules.AddAccessRule(new FileSystemAccessRule("Users",
                    FileSystemRights.FullControl,
                    InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                    PropagationFlags.InheritOnly,
                    AccessControlType.Allow));
                di.SetAccessControl(securityRules);

                return System.IO.Directory.GetFiles(path, "*.bak").Select(fullPath => new FileInfo(fullPath)).OrderByDescending(p=>p.CreationTime).ToList();
            }
        }

        /// <summary>
        /// Creates a backup of the database
        /// </summary>
        public virtual void BackupDatabase()
        {
            var fileName = string.Format(
                "{0}database_{1}_{2}.bak",
                GetBackupsPath,
                DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss"),
                CommonHelper.GenerateRandomDigitCode(4));

            var commandText = string.Format(
                "BACKUP DATABASE [{0}] TO DISK = '{1}' WITH FORMAT, COMPRESSION",
                _dbContext.DbName(),
                fileName);


            _dbContext.ExecuteSqlCommand(commandText, true);
        }

        /// <summary>
        /// Restores the database from a backup
        /// </summary>
        /// <param name="backupFileName">The name of the backup file</param>
        public virtual void RestoreDatabase(string backupFileName)
        {
            var settings = new DataSettingsManager();
            var conn = new SqlConnectionStringBuilder(settings.LoadSettings().DataConnectionString)
            {
                InitialCatalog = "master"
            };

            using (var sqlConnectiononn = new SqlConnection(conn.ToString()))
            {
                var commandText = string.Format(
                    "DECLARE @ErrorMessage NVARCHAR(4000)\n" +
                    "ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE\n" +
                    "BEGIN TRY\n" +
                        "RESTORE DATABASE [{0}] FROM DISK = '{1}' WITH REPLACE\n" +
                    "END TRY\n" +
                    "BEGIN CATCH\n" +
                        "SET @ErrorMessage = ERROR_MESSAGE()\n" +
                    "END CATCH\n" +
                    "ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE\n" +
                    "IF (@ErrorMessage is not NULL)\n" +
                    "BEGIN\n" +
                        "RAISERROR (@ErrorMessage, 16, 1)\n" +
                    "END",
                    _dbContext.DbName(),
                    backupFileName);

                DbCommand dbCommand = new SqlCommand(commandText, sqlConnectiononn);
                if (sqlConnectiononn.State != ConnectionState.Open)
                    sqlConnectiononn.Open();
                dbCommand.ExecuteNonQuery();
            }

            //clear all pools
            SqlConnection.ClearAllPools();
        }

        /// <summary>
        /// Returns the path to the backup file
        /// </summary>
        /// <param name="backupFileName">The name of the backup file</param>
        /// <returns>The path to the backup file</returns>
        public virtual string GetBackupPath(string backupFileName)
        {
            return Path.Combine(GetBackupsPath, backupFileName);
        }

        #endregion
    }
}
