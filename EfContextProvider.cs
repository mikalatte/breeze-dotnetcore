using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Xml;
using System.Xml.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Microsoft.EntityFrameworkCore;

using Breeze.ContextProvider;
using Microsoft.Data.Edm.Csdl;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Configuration;
using BreezeCp = Breeze.ContextProvider;

namespace Breeze.ContextProvider.EFCore
{

    public interface IEFContextProvider
    {
        DbContext Context { get; }
    }

    // T is either a subclass of DbContext or a subclass of DbContext
    public class EFContextProvider<T> : Breeze.ContextProvider.ContextProvider, IEFContextProvider
        where T : DbContext
    {
        private IConfiguration _configuration;

        public EFContextProvider(T context, IConfiguration configuration)
        {
            _configuration = configuration;
            _context = context;
        }

        private void SetOriginalValues(Dictionary<Type, List<EntityInfo>> arg)
        {
            foreach (var type in arg.Keys)
            {
                var entityInfos = arg[type];
                foreach (var entityInfo in entityInfos)
                {
                    var originalValues = Context.Entry(entityInfo.Entity).GetDatabaseValues();

                    entityInfo.OriginalEntity = originalValues?.ToObject();
                }
            }

        }

        

        public DbContext Context
        {
            get { return _context; }

        }

        protected T _context;


        /// <summary>Gets the EntityConnection from the DbContext.</summary>
        public DbConnection EntityConnection
        {
            get
            {
                return (DbConnection)GetDbConnection();
            }
        }

        /// <summary>Gets the StoreConnection from the DbContext.</summary>
        public IDbConnection StoreConnection
        {
            get
            {
                return (GetDbConnection());
            }
        }

        /// <summary>Gets the current transaction, if one is in progress.</summary>
        public IDbTransaction EntityTransaction
        {
            get; private set;
        }


        /// <summary>Gets the EntityConnection from the DbContext.</summary>
        public override IDbConnection GetDbConnection()
        {
            return Context.Database.GetDbConnection();
        }

        /// <summary>
        /// Opens the DbConnection used by the Context.
        /// If the connection will be used outside of the DbContext, this method should be called prior to DbContext 
        /// initialization, so that the connection will already be open when the DbContext uses it.  This keeps
        /// the DbContext from closing the connection, so it must be closed manually.
        /// See http://blogs.msdn.com/b/diego/archive/2012/01/26/exception-from-dbcontext-api-entityconnection-can-only-be-constructed-with-a-closed-dbconnection.aspx
        /// </summary>
        /// <returns></returns>
        protected override void OpenDbConnection()
        {
            var ec = Context.Database.GetDbConnection();
            if (ec.State == ConnectionState.Closed) ec.Open();
        }

        protected override void CloseDbConnection()
        {
            if (_context != null)
            {
                var ec = Context.Database.GetDbConnection();
                ec.Close();
                ec.Dispose();
            }
        }

        // Override BeginTransaction so we can keep the current transaction in a property
        protected override IDbTransaction BeginTransaction(System.Data.IsolationLevel isolationLevel)
        {
            var conn = GetDbConnection();
            if (conn == null) return null;
            EntityTransaction = conn.BeginTransaction(isolationLevel);
            return EntityTransaction;

        }


        #region Base implementation overrides

        protected override string BuildJsonMetadata()
        {
            var json = JsonConvert.SerializeObject(Context.Model, new JsonSerializerSettings()
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            });
            var altMetadata = BuildAltJsonMetadata();
            if (altMetadata != null)
            {
                json = "{ \"altMetadata\": " + altMetadata + "," + json.Substring(1);
            }
            return json;
        }

        protected virtual string BuildAltJsonMetadata()
        {
            // default implementation
            return null; // "{ \"foo\": 8, \"bar\": \"xxx\" }";
        }

        protected override EntityInfo CreateEntityInfo()
        {
            return new EFEntityInfo();
        }

        public override object[] GetKeyValues(EntityInfo entityInfo)
        {
            return GetKeyValues(entityInfo.Entity);
        }

        public object[] GetKeyValues(object entity)
        {

            if (Context.Model.FindEntityType(entity.GetType()) == null)
                throw new ArgumentException("EntitySet not found for type " + entity.GetType());

            var entry = Context.Entry(entity);

            var keyProps = entry.Metadata.FindPrimaryKey().Properties.Select(p => entry.Property(p.Name))
                .ToArray();
            if (!entry.IsKeySet)
            {
                foreach (var prop in keyProps)
                {
                    prop.CurrentValue = Guid.NewGuid();
                }
            }
            var key = keyProps.Select(kp => kp.CurrentValue).ToArray();
            return key;
        }


        protected override void SaveChangesCore(SaveWorkState saveWorkState)
        {
            var saveMap = saveWorkState.SaveMap;
            SetOriginalValues(saveMap);
            var deletedEntities = ProcessSaves(saveMap);

            if (deletedEntities.Any())
            {
                ProcessAllDeleted(deletedEntities);
            }
            ProcessAutogeneratedKeys(saveWorkState.EntitiesWithAutoGeneratedKeys);

            int count;
            try
            {

                count = Context.SaveChanges(true);

            }
            catch (DbUpdateException e)
            {
                var entityErrors = new List<EntityError>();
                foreach (var eve in e.Entries)
                {
                    var entity = eve.Entity;
                    var entityTypeName = entity.GetType().FullName;
                    var keyValues = eve.Metadata.FindPrimaryKey().Properties
                        .Select(p => eve.Property(p.Name).CurrentValue).ToArray();



                    var entityError = new EntityError()
                    {
                        EntityTypeName = entityTypeName,
                        KeyValues = keyValues,
                        ErrorMessage = e.InnerException?.Message ?? e.Message,

                    };
                    entityErrors.Add(entityError);


                }
                saveWorkState.EntityErrors = entityErrors;

            }
            catch (DataException e)
            {
                var nextException = (Exception)e;
                while (nextException.InnerException != null)
                {
                    nextException = nextException.InnerException;
                }
                if (nextException == e)
                {
                    throw;
                }
                else
                {
                    //create a new exception that contains the toplevel exception
                    //but has the innermost exception message propogated to the top.
                    //For EF exceptions, this is often the most 'relevant' message.
                    throw new Exception(nextException.Message, e);
                }
            }
            catch (Exception)
            {
                throw;
            }

            saveWorkState.KeyMappings = UpdateAutoGeneratedKeys(saveWorkState.EntitiesWithAutoGeneratedKeys);
        }




        #endregion

        #region Save related methods

        private List<EFEntityInfo> ProcessSaves(Dictionary<Type, List<EntityInfo>> saveMap)
        {
            var deletedEntities = new List<EFEntityInfo>();
            foreach (var kvp in saveMap)
            {
                if (kvp.Value == null || kvp.Value.Count == 0) continue;  // skip GetEntitySetName if no entities
                var entityType = kvp.Key;
                foreach (EFEntityInfo entityInfo in kvp.Value)
                {
                    // entityInfo.EFContextProvider = this;  may be needed eventually.

                    ProcessEntity(entityInfo);
                    if (entityInfo.EntityState == BreezeCp.EntityState.Deleted)
                    {
                        deletedEntities.Add(entityInfo);
                    }
                }
            }
            return deletedEntities;
        }

        private void ProcessAllDeleted(List<EFEntityInfo> deletedEntities)
        {
            deletedEntities.ForEach(entityInfo =>
            {

                RestoreOriginal(entityInfo);
                var entry = GetObjectStateEntry(entityInfo.Entity);
                entry.State = Microsoft.EntityFrameworkCore.EntityState.Deleted;
                entityInfo.ObjectStateEntry = entry;
            });
        }

        private void ProcessAutogeneratedKeys(List<EntityInfo> entitiesWithAutoGeneratedKeys)
        {
            var tempKeys = entitiesWithAutoGeneratedKeys.Cast<EFEntityInfo>().Where(
                    entityInfo => entityInfo.AutoGeneratedKey.AutoGeneratedKeyType == AutoGeneratedKeyType.KeyGenerator)
                .Select(ei => new TempKeyInfo(ei))
                .ToList();
            if (tempKeys.Count == 0) return;
            if (this.KeyGenerator == null)
            {
                this.KeyGenerator = GetKeyGenerator();
            }
            this.KeyGenerator.UpdateKeys(tempKeys);
            tempKeys.ForEach(tki =>
            {
                // Clever hack - next 3 lines cause all entities related to tki.Entity to have 
                // their relationships updated. So all related entities for each tki are updated.
                // Basically we set the entity to look like a preexisting entity by setting its
                // entityState to unchanged.  This is what fixes up the relations, then we set it back to added
                // Now when we update the pk - all fks will get changed as well.  Note that the fk change will only
                // occur during the save.
                var entry = GetObjectStateEntry(tki.Entity);

                entry.State = Microsoft.EntityFrameworkCore.EntityState.Added;
                var val = ConvertValue(tki.RealValue, tki.Property.PropertyType);
                tki.Property.SetValue(tki.Entity, val, null);
            });
        }

        private IKeyGenerator GetKeyGenerator()
        {
            var generatorType = KeyGeneratorType.Value;
            return (IKeyGenerator)Activator.CreateInstance(generatorType, StoreConnection);
        }

        private EntityInfo ProcessEntity(EFEntityInfo entityInfo)
        {
            EntityEntry ose;
            if (entityInfo.EntityState == BreezeCp.EntityState.Modified)
            {
                ose = HandleModified(entityInfo);
            }
            else if (entityInfo.EntityState == BreezeCp.EntityState.Added)
            {
                ose = HandleAdded(entityInfo);
            }
            else if (entityInfo.EntityState == BreezeCp.EntityState.Deleted)
            {
                // for 1st pass this does NOTHING 
                ose = HandleDeletedPart1(entityInfo);
            }
            else
            {
                // needed for many to many to get both ends into the DbContext
                ose = HandleUnchanged(entityInfo);
            }
            entityInfo.ObjectStateEntry = ose;
            return entityInfo;
        }

        private EntityEntry HandleAdded(EFEntityInfo entityInfo)
        {
            var entry = GetObjectStateEntry(entityInfo.Entity);
            if (entityInfo.AutoGeneratedKey != null)
            {
                entityInfo.AutoGeneratedKey.TempValue = GetOsePropertyValue(entry, entityInfo.AutoGeneratedKey.PropertyName);
            }
            entry.State = Microsoft.EntityFrameworkCore.EntityState.Added;
            return entry;
        }

        private EntityEntry HandleModified(EFEntityInfo entityInfo)
        {
            var entry = GetObjectStateEntry(entityInfo.Entity);
            // EntityState will be changed to modified during the update from the OriginalValuesMap
            // Do NOT change this to EntityState.Modified because this will cause the entire record to update.

            entry.State = Microsoft.EntityFrameworkCore.EntityState.Modified;


            return entry;
        }

        private EntityEntry HandleUnchanged(EFEntityInfo entityInfo)
        {
            var entry = GetObjectStateEntry(entityInfo.Entity);
            entry.State = Microsoft.EntityFrameworkCore.EntityState.Unchanged;
            return entry;
        }

        private EntityEntry HandleDeletedPart1(EntityInfo entityInfo)
        {
            return null;
        }

        private EntityInfo RestoreOriginal(EntityInfo entityInfo)
        {
            // fk's can get cleared depending on the order in which deletions occur -
            // EF needs the original values of these fk's under certain circumstances - ( not sure entirely what these are). 
            // so we restore the original fk values right before we attach the entity 
            // shouldn't be any side effects because we delete it immediately after.
            // ??? Do concurrency values also need to be restored in some cases 
            // This method restores more than it actually needs to because we don't
            // have metadata easily avail here, but usually a deleted entity will
            // not have much in the way of OriginalValues.
            if (entityInfo.OriginalValuesMap == null || entityInfo.OriginalValuesMap.Keys.Count == 0)
            {
                return entityInfo;
            }
            var entity = entityInfo.Entity;
            var entityType = entity.GetType();

            var keyPropertyNames = Context.Entry(entity).Metadata.FindPrimaryKey().Properties.Select(p => p.Name).ToArray();
            var ovl = entityInfo.OriginalValuesMap.ToList();
            for (var i = 0; i < ovl.Count; i++)
            {
                var kvp = ovl[i];
                var propName = kvp.Key;
                // keys should be ignored
                if (keyPropertyNames.Contains(propName)) continue;
                var pi = entityType.GetProperty(propName);
                // unmapped properties should be ignored.
                if (pi == null) continue;
                var nnPropType = TypeFns.GetNonNullableType(pi.PropertyType);
                // presumption here is that only a predefined type could be a fk or concurrency property
                if (TypeFns.IsPredefinedType(nnPropType))
                {
                    SetPropertyValue(entity, propName, kvp.Value);
                }
            }

            return entityInfo;
        }


        //private static void UpdateOriginalValues(EntityEntry entry, EntityInfo entityInfo)
        //{
        //    var originalValuesMap = entityInfo.OriginalValuesMap;
        //    if (originalValuesMap == null || originalValuesMap.Keys.Count == 0) return;

        //    var originalValuesRecord = entry.GetUpdatableOriginalValues();
        //    originalValuesMap.ToList().ForEach(kvp => {
        //        var propertyName = kvp.Key;
        //        var originalValue = kvp.Value;

        //        try
        //        {
        //            entry.SetModifiedProperty(propertyName);
        //            if (originalValue is JObject)
        //            {
        //                // only really need to perform updating original values on key properties
        //                // and a complex object cannot be a key.
        //            }
        //            else
        //            {
        //                var ordinal = originalValuesRecord.GetOrdinal(propertyName);
        //                var fieldType = originalValuesRecord.GetFieldType(ordinal);
        //                var originalValueConverted = ConvertValue(originalValue, fieldType);

        //                originalValuesRecord.SetValue(ordinal, originalValueConverted);

        //                // Below code removed by Steve, 2018-02-01, as it seems the hack is no longer needed (and causes problems)
        //                //if (originalValueConverted == null) {
        //                //  // bug - hack because of bug in EF - see 
        //                //  // http://social.msdn.microsoft.com/Forums/nl/adodotnetentityframework/thread/cba1c425-bf82-4182-8dfb-f8da0572e5da
        //                //  var temp = entry.CurrentValues[ordinal];
        //                //  entry.CurrentValues.SetDBNull(ordinal);
        //                //  entry.ApplyOriginalValues(entry.Entity);
        //                //  entry.CurrentValues.SetValue(ordinal, temp);
        //                //} else {
        //                //  originalValuesRecord.SetValue(ordinal, originalValueConverted);
        //                //}
        //            }
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message.Contains(" part of the entity's key"))
        //            {
        //                throw;
        //            }
        //            else
        //            {
        //                // this can happen for "custom" data entity properties.
        //            }
        //        }
        //    });

        //}

        private List<KeyMapping> UpdateAutoGeneratedKeys(List<EntityInfo> entitiesWithAutoGeneratedKeys)
        {
            // where clause is necessary in case the Entities were suppressed in the beforeSave event.
            var keyMappings = entitiesWithAutoGeneratedKeys.Cast<EFEntityInfo>()
                .Where(entityInfo => entityInfo.ObjectStateEntry != null)
                .Select(entityInfo =>
                {
                    var autoGeneratedKey = entityInfo.AutoGeneratedKey;
                    if (autoGeneratedKey.AutoGeneratedKeyType == AutoGeneratedKeyType.Identity)
                    {
                        autoGeneratedKey.RealValue = GetOsePropertyValue(entityInfo.ObjectStateEntry, autoGeneratedKey.PropertyName);
                    }
                    return new KeyMapping()
                    {
                        EntityTypeName = entityInfo.Entity.GetType().FullName,
                        TempValue = autoGeneratedKey.TempValue,
                        RealValue = autoGeneratedKey.RealValue
                    };
                });
            return keyMappings.ToList();
        }

        private Object GetOsePropertyValue(EntityEntry ose, String propertyName)
        {
            return ose.Property(propertyName).CurrentValue;
        }

        private void SetOsePropertyValue(EntityEntry ose, String propertyName, Object value)
        {
            ose.Property(propertyName).CurrentValue = value;
        }

        private void SetPropertyValue(Object entity, String propertyName, Object value)
        {
            var propInfo = entity.GetType().GetProperty(propertyName,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            // exit if unmapped property.
            if (propInfo == null) return;
            if (propInfo.CanWrite)
            {
                var val = ConvertValue(value, propInfo.PropertyType);
                propInfo.SetValue(entity, val, null);
            }
            else
            {
                throw new Exception(String.Format("Unable to write to property '{0}' on type: '{1}'", propertyName,
                    entity.GetType()));
            }
        }

        private static Object ConvertValue(Object val, Type toType)
        {
            Object result;
            if (val == null) return val;
            if (toType == val.GetType()) return val;
            var nnToType = TypeFns.GetNonNullableType(toType);

            if (nnToType.IsEnum && val is string)
            {
                result = Enum.Parse(nnToType, val as string, true);
            }
            else if (typeof(IConvertible).IsAssignableFrom(nnToType))
            {
                result = Convert.ChangeType(val, nnToType, System.Threading.Thread.CurrentThread.CurrentCulture);
            }
            else if (val is JObject)
            {
                var serializer = new JsonSerializer();
                result = serializer.Deserialize(new JTokenReader((JObject)val), toType);
            }
            else
            {
                // Guids fail above - try this
                TypeConverter typeConverter = TypeDescriptor.GetConverter(toType);
                if (typeConverter.CanConvertFrom(val.GetType()))
                {
                    result = typeConverter.ConvertFrom(val);
                }
                else if (val is DateTime && toType == typeof(DateTimeOffset))
                {
                    // handle case where JSON deserializes to DateTime, but toType is DateTimeOffset.  DateTimeOffsetConverter doesn't work!
                    result = new DateTimeOffset((DateTime)val);
                }
                else
                {
                    result = val;
                }
            }
            return result;
        }

        //private EntityEntry GetOrAddObjectStateEntry(EFEntityInfo entityInfo)
        //{
        //    EntityEntry entry;

        //    return AddObjectStateEntry(entityInfo);



        //}

        //private EntityEntry AddObjectStateEntry(EFEntityInfo entityInfo)
        //{
        //    var ose = GetObjectStateEntry(entityInfo.Entity, false);
        //    if (ose != null) return ose;
        //    DbContext.
        //    // Attach has lots of side effect - add has far fewer.
        //    return GetObjectStateEntry(entityInfo);
        //}

        //private EntityEntry AttachObjectStateEntry(EFEntityInfo entityInfo)
        //{
        //    DbContext.AttachTo(entityInfo.EntitySetName, entityInfo.Entity);
        //    // Attach has lots of side effect - add has far fewer.
        //    return GetObjectStateEntry(entityInfo);
        //}

        //private EntityEntry GetObjectStateEntry(EFEntityInfo entityInfo)
        //{
        //    return GetObjectStateEntry(entityInfo.Entity);
        //}

        private EntityEntry GetObjectStateEntry(Object entity, bool errorIfNotFound = true)
        {
            EntityEntry entry = null;
            try
            {
                entry = Context.Entry(entity);
                return entry;
            }
            catch (Exception e)
            {
                if (errorIfNotFound)
                    throw new Exception("unable to add to context: " + entity);
            }
            return entry;
        }


        #endregion

        #region Metadata methods







        protected String GetConnectionStringFromConfig(String connectionName)
        {
            var item = _configuration.GetConnectionString(connectionName);
            return item;
        }

        #endregion



        //var entityTypes = key.MetadataWorkspace.GetItems<EntityType>(DataSpace.OSpace);
        //// note CSpace below - not OSpace - evidently the entityContainer is only in the CSpace.
        //var entitySets = key.MetadataWorkspace.GetItems<EntityContainer>(DataSpace.CSpace)
        //    .SelectMany(c => c.BaseEntitySets.Where(es => es.ElementType.BuiltInTypeKind == BuiltInTypeKind.EntityType)).ToList();

        //private EntitySet GetDefaultEntitySet(EntityType cspaceEntityType) {
        //  var entitySet = _cspaceContainers.First().BaseEntitySets.OfType<EntitySet>().Where(es => es.ElementType == cspaceEntityType).FirstOrDefault();
        //  if (entitySet == null) {
        //    var baseEntityType = cspaceEntityType.BaseType as EntityType;
        //    if (baseEntityType != null) {
        //      return GetDefaultEntitySet(baseEntityType);
        //    } else {
        //      return null;
        //    }
        //  }
        //  return entitySet;
        //}


        //// from DF


        private const string ResourcePrefix = @"res://";


    }




    public class EFEntityInfo : EntityInfo
    {
        internal EFEntityInfo()
        {
        }

        internal String EntitySetName;
        internal EntityEntry ObjectStateEntry;
    }

    public class EFEntityError : EntityError
    {
        public EFEntityError(EntityInfo entityInfo, String errorName, String errorMessage, String propertyName)
        {


            if (entityInfo != null)
            {
                this.EntityTypeName = entityInfo.Entity.GetType().FullName;
                this.KeyValues = GetKeyValues(entityInfo);
            }
            ErrorName = errorName;
            ErrorMessage = errorMessage;
            PropertyName = propertyName;
        }

        private Object[] GetKeyValues(EntityInfo entityInfo)
        {
            return entityInfo.ContextProvider.GetKeyValues(entityInfo);
        }
    }

}