using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Data.SqlClient;
using System.Data;

namespace Helper.DBHelper
{
    public class EntityHelper
    {
        public static SqlParameter[] ToSqlParameters<T>(T entity)
        {
            List<SqlParameter> list = null;

            Func<T, List<SqlParameter>> fnc = getEntityConverter<T>();

            list = fnc(entity);

            return list.ToArray();
        }

        public static List<T> ToListOfEntity<T>(SqlDataReader reader)
        {
            List<T> list = new List<T>();

            Func<IDataRecord, T> fnc = getReaderConverter<T>((IDataRecord)reader);

            while (reader.Read())
            {
                list.Add(fnc(reader));
            }

            return list;
        }

        #region Setter template
        private static Action<T, String> createPropertySetter<T>(String key)
        {
            Action<T, String> setter = null;

            try
            {
                System.Reflection.PropertyInfo prop = typeof(T).GetProperty(key, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.IgnoreCase);

                if (prop == null)
                {
                    return null;
                }


                ParameterExpression peInstance = Expression.Variable(typeof(T));
                MemberExpression meInstanceProp = Expression.Property(peInstance, prop);

                ParameterExpression peValue = Expression.Variable(typeof(String));
                BinaryExpression beAssignProp = null;

                if (prop.PropertyType == typeof(String))
                {
                    beAssignProp = BinaryExpression.Assign(meInstanceProp, peValue);
                }
                else
                {
                    Type tCaster = typeof(StringConverter);
                    ParameterExpression peCaster = Expression.Variable(tCaster);

                    //get constructor for StringCaster(string s)
                    System.Reflection.ConstructorInfo ci = tCaster.GetConstructor(new Type[] { typeof(string) });

                    //new StringCaster(value) for casting and assigning value
                    beAssignProp = BinaryExpression.Assign(meInstanceProp, Expression.Convert(Expression.New(ci, peValue), prop.PropertyType));
                }

                var body = Expression.Block(new Expression[] { beAssignProp });

                var lamda = Expression.Lambda<Action<T, String>>(body, new ParameterExpression[] { peInstance, peValue });
                setter = lamda.Compile();
            }
            catch (System.Reflection.AmbiguousMatchException amex)
            {
                //matched more than one property
                System.Diagnostics.Debug.WriteLine("Found more than one property): " + amex.ToString());
            }
            catch (ArgumentNullException aex)
            {
                //Invalid properties (name is null)
                System.Diagnostics.Debug.WriteLine("Invalid property name is null): " + aex.ToString());
            }
            catch (Exception e)
            {
                throw e;
            }

            return setter;
        }

        private static ConcurrentDictionary<Type, Delegate> _expressionEntityCache = new ConcurrentDictionary<Type, Delegate>();
        private static Func<i, List<SqlParameter>> getEntityConverter<i>()
        {
            Type typeIn = typeof(i);
            Type typeOut = typeof(List<SqlParameter>);
            Type typeSqlParameter = typeof(SqlParameter);

            Delegate creator = null;

            if (!_expressionEntityCache.TryGetValue(typeIn, out creator))
            {
                List<Expression> statements = new List<Expression>();

                ParameterExpression peIn = Expression.Variable(typeIn);
                ParameterExpression peOut = Expression.Variable(typeOut);
                ParameterExpression peGeneric = Expression.Variable(typeSqlParameter);

                statements.Add(Expression.Assign(peOut, Expression.New(typeOut)));

                System.Reflection.PropertyInfo piParameterName;
                piParameterName = typeSqlParameter.GetProperty("ParameterName");
                
                System.Reflection.PropertyInfo piValue;
                piValue = typeSqlParameter.GetProperty("Value");

                BinaryExpression beAssignProp = null;
                Type customAttrMapper = typeof(System.Data.Linq.Mapping.ColumnAttribute);
                System.Reflection.PropertyInfo[] props = typeIn.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
                foreach (System.Reflection.PropertyInfo piIn in props)
                {
                    statements.Add(Expression.Assign(peGeneric, Expression.New(typeSqlParameter)));

                    MemberExpression meGenericParam = Expression.Property(peGeneric, piParameterName);

                    string paramName = string.Empty;
                    System.Data.Linq.Mapping.ColumnAttribute[] attrs = (System.Data.Linq.Mapping.ColumnAttribute[])piIn.GetCustomAttributes(customAttrMapper, false);

                    if (attrs.Length > 0)
                        paramName = "@" + attrs[0].Name;
                    else
                        paramName = "@" + piIn.Name;

                    beAssignProp = BinaryExpression.Assign(meGenericParam, Expression.Constant(paramName));

                    statements.Add(beAssignProp);

                    MemberExpression meGenericValue = Expression.Property(peGeneric, piValue);
                    MemberExpression meInProp = Expression.Property(peIn, piIn);
                    UnaryExpression unInProp = Expression.Convert(meInProp, typeof(object));

                    beAssignProp = BinaryExpression.Assign(meGenericValue, unInProp);

                    statements.Add(beAssignProp);

                    MethodCallExpression mce = Expression.Call(peOut, typeOut.GetMethod("Add", new Type[] { typeSqlParameter }), new Expression[] { peGeneric });
                    statements.Add(mce);
                }

                var returnStatement = peOut;
                statements.Add(returnStatement);

                var body = Expression.Block(new[] { peOut, peGeneric }, statements.ToArray());

                var lamda = Expression.Lambda<Func<i, List<SqlParameter>>>(body, peIn);
                creator = lamda.Compile();

                _expressionEntityCache[typeIn] = creator;
            }

            return (Func<i, List<SqlParameter>>)creator;
        }

        private static ConcurrentDictionary<Type, Delegate> _expressionReaderCache = new ConcurrentDictionary<Type, Delegate>();
        private static Func<IDataRecord, o> getReaderConverter<o>(IDataRecord record)
        {
            Type typeIn = typeof(IDataRecord);
            Type typeOut = typeof(o);

            Delegate creator = null;

            if (!_expressionReaderCache.TryGetValue(typeOut, out creator))
            {
                List<Expression> statements = new List<Expression>();

                ParameterExpression peIn = Expression.Variable(typeIn);
                ParameterExpression peOut = Expression.Variable(typeOut);

                statements.Add(Expression.Assign(peOut, Expression.New(typeOut)));
                
                Type customAttrMapper = typeof(System.Data.Linq.Mapping.ColumnAttribute);
                BinaryExpression beAssignProp = null;
                System.Reflection.PropertyInfo[] props = typeOut.GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
                foreach (System.Reflection.PropertyInfo piOut in props)
                {
                    int columnIndex = -1;
                                        
                    try
                    {
                        //find column index
                        System.Data.Linq.Mapping.ColumnAttribute[] attrs = (System.Data.Linq.Mapping.ColumnAttribute[])piOut.GetCustomAttributes(customAttrMapper, false);

                        if(attrs.Length > 0)
                            columnIndex = record.GetOrdinal(attrs[0].Name);
                        else
                            columnIndex = record.GetOrdinal(piOut.Name);
                        
                    }
                    catch(IndexOutOfRangeException iorEx)
                    {
                        //skip if not match to any column
                        continue;
                    }
                    
                    ConstantExpression ce = Expression.Constant(columnIndex, typeof(int));
                    MethodCallExpression mce = null;
                    MemberExpression meOut = Expression.Property(peOut, piOut.Name);

                    if (piOut.PropertyType.IsGenericType)
                    {
                        mce = Expression.Call(peIn, typeIn.GetMethod("Get" + Nullable.GetUnderlyingType(piOut.PropertyType).Name), new Expression[] { ce });
                        
                        beAssignProp = BinaryExpression.Assign(meOut, Expression.Convert(mce, piOut.PropertyType));
                    }
                    else
                    {
                        mce = Expression.Call(peIn, typeIn.GetMethod("Get" + piOut.PropertyType.Name), new Expression[] { ce });
                        
                        beAssignProp = BinaryExpression.Assign(meOut, mce);
                    }
                    
                    MethodCallExpression mceCheck = Expression.Call(peIn, typeIn.GetMethod("IsDBNull"), new Expression[] { ce });
                    ConditionalExpression conde = Expression.IfThen(Expression.Not(mceCheck), beAssignProp);

                    //statements.Add(beAssignProp);
                    statements.Add(conde);
                }

                var returnStatement = peOut;
                statements.Add(returnStatement);

                var body = Expression.Block(new[] { peOut }, statements.ToArray());

                var lamda = Expression.Lambda<Func<IDataRecord, o>>(body, peIn);
                creator = lamda.Compile();

                _expressionReaderCache[typeOut] = creator;
            }

            return (Func<IDataRecord, o>)creator;
        }
        #endregion
        #region Helper class
        private sealed class StringConverter
        {
            private string _s = null;

            public StringConverter(string s)
            {
                _s = s;
            }

            //User-defined conversion from StringCaster (string) to double 
            public static implicit operator double(StringConverter s)
            {
                return Convert.ToDouble(s.ToString());
            }

            //User-defined conversion from StringCaster (string) to int
            public static implicit operator int(StringConverter s)
            {
                return Convert.ToInt32(s.ToString());
            }

            //User-defined conversion from StringCaster (string) to bool
            public static implicit operator bool(StringConverter s)
            {
                return Convert.ToBoolean(s.ToString());
            }

            public override string ToString()
            {
                if (_s == null)
                    throw new NullReferenceException();

                return _s;
            }
        }
        #endregion
    }
}