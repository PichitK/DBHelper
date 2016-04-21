using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Helper.DBHelper
{
    public class SqlClientHelper : IDisposable
    {
        private string _strCon = "";
        public string ConnectionString
        {
            get { return this._strCon; }
            set
            {
                this._strCon = value;

                this.Connection = null; // Releases current connection

                if ((this._strCon != null) && (this._strCon != string.Empty))
                {
                    this.Connection = new SqlConnection(this._strCon);
                }
            }
        }

        private SqlConnection _con = null;
        public SqlConnection Connection
        {
            get
            {
                if (this._con == null)
                {
                    if ((this.ConnectionString != null) && (this.ConnectionString != string.Empty))
                        throw new DBHelperException("Invalid connection string");

                    this._con = new SqlConnection(this._strCon);
                }

                return this._con;
            }

            set
            {
                if (value == null)
                {
                    if (this._con != null)
                    {
                        if (this._con.State == ConnectionState.Open)
                        {
                            this.Rollback(); //Clear pending transaction data before close
                            this._con.Close();
                        }
                        this._con.Dispose();
                    }
                }
                else
                {
                    this._con = value;
                }
            }
        }

        private SqlTransaction _transaction = null;
        public SqlTransaction Transaction
        {
            get { return this._transaction; }
            set { this._transaction = value; }
        }

        public const int COMMAND_TIMEOUT_DEFAULT = -1;
        public int CommandTimeout { get; set; }

        private SqlClientHelper()
        {
            this.CommandTimeout = COMMAND_TIMEOUT_DEFAULT;
        }

        public SqlClientHelper(string connectionString)
        {
            this.ConnectionString = connectionString;
            this.CommandTimeout = COMMAND_TIMEOUT_DEFAULT;
        }

        public void Dispose()
        {
            if (this._transaction != null)
                this._transaction.Dispose();

            if (this._con != null)
            {
                if (this._con.State == ConnectionState.Open)
                    this._con.Close();
                this._con.Dispose();
            }

            GC.SuppressFinalize(this);

        }

        public void BeginTransaction()
        {
            if (this.Connection.State != ConnectionState.Open)
                this.Connection.Open();
            this.Transaction = this.Connection.BeginTransaction();
        }

        public void Rollback()
        {
            if (this.Transaction != null)
            {
                this.Transaction.Rollback();
                this.Transaction.Dispose();
                this.Transaction = null;
            }
        }

        public void Commit()
        {
            if (this.Transaction != null)
            {
                this.Transaction.Commit();
                this.Transaction.Dispose();
                this.Transaction = null;
            }
        }

        public static bool HasColumn(IDataRecord reader, string columnName)
        {
            try
            {
                return (reader.GetOrdinal(columnName) >= 0);
            }
            catch (Exception)
            {
                return false;
            }
        }

        public int ExecuteNonQuery(string commandText, CommandType type)
        {
            return this.ExecuteNonQuery(commandText, type, null);
        }

        public int ExecuteNonQuery<T>(string commandText, CommandType type, T entity)
        {
            if (entity != null)
            {
                SqlParameter[] parameters = null;

                try
                {
                    parameters = EntityHelper.ToSqlParameters<T>(entity);
                }
                catch (Exception ex)
                {
                    throw new DBHelperException("Cannot convert " + typeof(T).ToString() + " to SQLParameter", ex);
                }

                return this.ExecuteNonQuery(commandText, type, parameters);
            }
            else
            {
                return this.ExecuteNonQuery(commandText, type, null);
            }
        }

        public int ExecuteNonQuery(string commandText, CommandType type, SqlParameter[] parameters)
        {
            if (String.IsNullOrEmpty(this.ConnectionString))
                throw new ArgumentNullException("Invalid connection string");

            int affectedRow = 0;

            try
            {
                using (SqlCommand cmd = new SqlCommand(commandText, this.Connection))
                {
                    cmd.CommandType = type;

                    SqlParameter _return = new SqlParameter("@_sp_return", SqlDbType.Int);
                    _return.Direction = ParameterDirection.ReturnValue;

                    if (parameters != null)
                    {
                        foreach(var p in parameters)
                        {
                            if (p.Value == null)
                                p.Value = DBNull.Value;
                        }
                        
                            cmd.Parameters.AddRange(parameters);
                    }

                    cmd.Parameters.Add(_return);
                    
                    if (this.Connection.State == ConnectionState.Closed)
                        this.Connection.Open();

                    if (this.Transaction != null)
                        cmd.Transaction = this.Transaction;

                    affectedRow = cmd.ExecuteNonQuery();

                    if (type == CommandType.StoredProcedure)
                    {
                        affectedRow = (int)_return.Value;
                    }

                    cmd.Parameters.Clear();
                }
            }
            catch (SqlException sqex)
            {
                throw sqex;
            }
            finally
            {
                if (this.Transaction == null)
                    this.Connection.Close();
            }

            return affectedRow;
        }

        public Object ExecuteScalar(string commandText, CommandType type)
        {
            return this.ExecuteScalar(commandText, type, null);
        }

        public Object ExecuteScalar<T>(string commandText, CommandType type, T entity)
        {
            if (entity != null)
            {
                SqlParameter[] parameters = null;

                try
                {
                    parameters = EntityHelper.ToSqlParameters<T>(entity);
                }
                catch (Exception ex)
                {
                    throw new DBHelperException("Cannot convert " + typeof(T).ToString() + " to SQLParameter", ex);
                }

                return this.ExecuteScalar(commandText, type, parameters);
            }
            else
            {
                return this.ExecuteScalar(commandText, type, null);
            }
        }

        public Object ExecuteScalar(string commandText, CommandType type, SqlParameter[] parameters)
        {
            if (String.IsNullOrEmpty(this.ConnectionString))
                throw new ArgumentNullException("Invalid connection string");

            Object result = 0;

            try
            {
                using (SqlCommand cmd = new SqlCommand(commandText, this.Connection))
                {
                    cmd.CommandType = type;

                    if (parameters != null)
                    {
                        foreach (var p in parameters)
                        {
                            if (p.Value == null)
                                p.Value = DBNull.Value;
                        }

                        cmd.Parameters.AddRange(parameters);
                    }

                    if (this.Connection.State == ConnectionState.Closed)
                        this.Connection.Open();

                    if (this.Transaction != null)
                        cmd.Transaction = this.Transaction;

                    if (this.CommandTimeout != COMMAND_TIMEOUT_DEFAULT)
                        cmd.CommandTimeout = this.CommandTimeout;

                    result = cmd.ExecuteScalar();
                    cmd.Parameters.Clear();
                }
            }
            catch (SqlException sqex)
            {
                throw sqex;
            }
            finally
            {
                if (this.Transaction == null)
                    this.Connection.Close();
            }

            return result;
        }

        public DataSet Execute(string selectCommand, CommandType type)
        {
            return this.Execute(selectCommand, type, null);
        }

        public DataSet Execute<T>(string selectCommand, CommandType type, T entity)
        {
            if (entity != null)
            {
                SqlParameter[] parameters = null;

                try
                {
                    parameters = EntityHelper.ToSqlParameters<T>(entity);
                }
                catch (Exception ex)
                {
                    throw new DBHelperException("Cannot convert " + typeof(T).ToString() + " to SQLParameter", ex);
                }

                return this.Execute(selectCommand, type, parameters);
            }
            else
            {
                return this.Execute(selectCommand, type, null);
            }
        }

        public DataSet Execute(string selectCommand, CommandType type, SqlParameter[] parameters)
        {
            if (String.IsNullOrEmpty(this.ConnectionString))
                throw new ArgumentNullException("Invalid connection string");

            DataSet ds = new DataSet();

            using (SqlCommand cmd = new SqlCommand(selectCommand, this.Connection))
            {
                cmd.CommandType = type;

                if (parameters != null)
                {
                    foreach (var p in parameters)
                    {
                        if (p.Value == null)
                            p.Value = DBNull.Value;
                    }

                    cmd.Parameters.AddRange(parameters);
                }

                if (this.CommandTimeout != COMMAND_TIMEOUT_DEFAULT)
                    cmd.CommandTimeout = this.CommandTimeout;

                SqlDataAdapter da = new SqlDataAdapter(cmd);

                da.Fill(ds);
                cmd.Parameters.Clear();
            }

            return ds;
        }

        public IList<T> Bind<T>(string selectCommand, CommandType type)
        {
            return this.Bind<T>(selectCommand, type, null);
        }

        public IList<T> Bind<T, K>(string selectCommand, CommandType type, K entity)
        {
            if (entity != null)
            {
                SqlParameter[] parameters = null;

                try
                {
                    parameters = EntityHelper.ToSqlParameters<K>(entity);
                }
                catch (Exception ex)
                {
                    throw new DBHelperException("Cannot convert " + typeof(K).ToString() + " to SQLParameter", ex);
                }

                return this.Bind<T>(selectCommand, type, parameters);
            }
            else
            {
                return this.Bind<T>(selectCommand, type, null);
            }
        }

        public IList<T> Bind<T>(string selectCommand, CommandType type, SqlParameter[] parameters)
        {
            if (String.IsNullOrEmpty(this.ConnectionString))
                throw new ArgumentNullException("Invalid connection string");

            List<T> list = null;
            using (SqlCommand cmd = new SqlCommand(selectCommand, this.Connection))
            {
                cmd.CommandType = type;

                if (parameters != null)
                {
                    foreach (var p in parameters)
                    {
                        if (p.Value == null)
                            p.Value = DBNull.Value;
                    }

                    cmd.Parameters.AddRange(parameters);
                }

                bool bOpenFromLocal = false;

                try
                {
                    if (this.Connection.State == ConnectionState.Closed)
                    {
                        this.Connection.Open();
                        bOpenFromLocal = true;
                    }

                    if (this.CommandTimeout != COMMAND_TIMEOUT_DEFAULT)
                        cmd.CommandTimeout = this.CommandTimeout;

                    using (SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.Default))
                    {
                        list = EntityHelper.ToListOfEntity<T>(reader);
                    }

                    cmd.Parameters.Clear();
                }
                finally
                {
                    if (bOpenFromLocal)
                        this.Connection.Close();
                }
            }

            return list;
        }

        public class DBHelperException : Exception
        {
            public const int DEFAULT_ERROR = -1;
            public int ErrorCode { get; private set; }

            public DBHelperException(int code, string message)
                : base(message)
            {
                this.ErrorCode = code;
            }

            public DBHelperException(string message)
                : base(message)
            {
                this.ErrorCode = DEFAULT_ERROR;
            }

            public DBHelperException(string message, Exception innerException)
                : base(message, innerException)
            {
                this.ErrorCode = DEFAULT_ERROR;
            }
        }
    }
}
