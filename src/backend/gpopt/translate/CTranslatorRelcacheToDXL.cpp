//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorRelcacheToDXL.cpp
//
//	@doc:
//		Class translating relcache entries into DXL objects
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "cdb/cdbhash.h"
#include "access/heapam.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_proc.h"

#include "cdb/cdbpartition.h"
#include "catalog/namespace.h"
#include "catalog/pg_statistic.h"

#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdScCmp.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/CMDCastGPDB.h"
#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/md/CMDScCmpGPDB.h"

#include "gpopt/translate/CTranslatorUtils.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "gpos/base.h"
#include "gpos/error/CException.h"

#include "naucrates/exception.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CDXLColStats.h"

#include "gpopt/base/CUtils.h"

#include "gpopt/gpdbwrappers.h"

using namespace gpdxl;
using namespace gpopt;


static 
const ULONG rgulCmpTypeMappings[][2] = 
{
	{IMDType::EcmptEq, CmptEq},
	{IMDType::EcmptNEq, CmptNEq},
	{IMDType::EcmptL, CmptLT},
	{IMDType::EcmptG, CmptGT},
	{IMDType::EcmptGEq, CmptGEq},
	{IMDType::EcmptLEq, CmptLEq}
};

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveObject
//
//	@doc:
//		Retrieve a metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveObject
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	IMDCacheObject *pmdcacheobj = NULL;
	GPOS_ASSERT(NULL != md_accessor);

#ifdef FAULT_INJECTOR
	gpdb::InjectFaultInOptTasks(OptRelcacheTranslatorCatalogAccess);
#endif // FAULT_INJECTOR

	switch(mdid->MdidType())
	{
		case IMDId::EmdidGPDB:
			pmdcacheobj = RetreiveMDObjGPDB(memory_pool, md_accessor, mdid);
			break;
		
		case IMDId::EmdidRelStats:
			pmdcacheobj = RetrieveRelStats(memory_pool, mdid);
			break;
		
		case IMDId::EmdidColStats:
			pmdcacheobj = RetrieveColStats(memory_pool, md_accessor, mdid);
			break;
		
		case IMDId::EmdidCastFunc:
			pmdcacheobj = RetreiveCast(memory_pool, mdid);
			break;
		
		case IMDId::EmdidScCmp:
			pmdcacheobj = RetreiveScCmp(memory_pool, mdid);
			break;
			
		default:
			break;
	}

	if (NULL == pmdcacheobj)
	{
		// no match found
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	return pmdcacheobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetreiveMDObjGPDB
//
//	@doc:
//		Retrieve a GPDB metadata object from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorRelcacheToDXL::RetreiveMDObjGPDB
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	GPOS_ASSERT(mdid->MdidType() == CMDIdGPDB::EmdidGPDB);

	OID oid = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(0 != oid);

	// find out what type of object this oid stands for

	if (gpdb::IndexExists(oid))
	{
		return RetrieveIndex(memory_pool, md_accessor, mdid);
	}

	if (gpdb::TypeExists(oid))
	{
		return RetrieveType(memory_pool, mdid);
	}

	if (gpdb::RelationExists(oid))
	{
		return RetrieveRel(memory_pool, md_accessor, mdid);
	}

	if (gpdb::OperatorExists(oid))
	{
		return RetrieveScOp(memory_pool, mdid);
	}

	if (gpdb::AggregateExists(oid))
	{
		return RetrieveAgg(memory_pool, mdid);
	}

	if (gpdb::FunctionExists(oid))
	{
		return RetrieveFunc(memory_pool, mdid);
	}

	if (gpdb::TriggerExists(oid))
	{
		return RetrieveTrigger(memory_pool, mdid);
	}

	if (gpdb::CheckConstraintExists(oid))
	{
		return RetrieveCheckConstraints(memory_pool, md_accessor, mdid);
	}

	// no match found
	return NULL;

}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetRelName
//
//	@doc:
//		Return a relation name
//
//---------------------------------------------------------------------------
CMDName *
CTranslatorRelcacheToDXL::GetRelName
	(
	IMemoryPool *memory_pool,
	Relation rel
	)
{
	GPOS_ASSERT(NULL != rel);
	CHAR *szRelName = NameStr(rel->rd_rel->relname);
	CWStringDynamic *pstrRelName = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szRelName);
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrRelName);
	GPOS_DELETE(pstrRelName);
	return mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelIndexInfo
//
//	@doc:
//		Return the indexes defined on the given relation
//
//---------------------------------------------------------------------------
MDIndexInfoPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelIndexInfo
	(
	IMemoryPool *memory_pool,
	Relation rel
	)
{
	GPOS_ASSERT(NULL != rel);

	if (gpdb::RelPartIsNone(rel->rd_id) || gpdb::IsLeafPartition(rel->rd_id))
	{
		return RetrieveRelIndexInfoForNonPartTable(memory_pool, rel);
	}
	else if (gpdb::RelPartIsRoot(rel->rd_id))
	{
		return RetrieveRelIndexInfoForPartTable(memory_pool, rel);
	}
	else  
	{
		// interior partition: do not consider indexes
		MDIndexInfoPtrArray *md_index_info_array = GPOS_NEW(memory_pool) MDIndexInfoPtrArray(memory_pool);
		return md_index_info_array;
	}
}

// return index info list of indexes defined on a partitioned table
MDIndexInfoPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelIndexInfoForPartTable
	(
	IMemoryPool *memory_pool,
	Relation relRoot
	)
{
	MDIndexInfoPtrArray *md_index_info_array = GPOS_NEW(memory_pool) MDIndexInfoPtrArray(memory_pool);

	// root of partitioned table: aggregate index information across different parts
	List *plLogicalIndexInfo = RetrievePartTableIndexInfo(relRoot);

	ListCell *lc = NULL;

	ForEach (lc, plLogicalIndexInfo)
	{
		LogicalIndexInfo *logicalIndexInfo = (LogicalIndexInfo *) lfirst(lc);
		OID index_oid = logicalIndexInfo->logicalIndexOid;

		// only add supported indexes
		Relation relIndex = gpdb::GetRelation(index_oid);

		if (NULL == relIndex)
		{
			WCHAR wsz[1024];
			CWStringStatic str(wsz, 1024);
			COstreamString oss(&str);
			oss << (ULONG) index_oid;
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, str.GetBuffer());
		}

		GPOS_ASSERT(NULL != relIndex->rd_indextuple);

		GPOS_TRY
		{
			if (IsIndexSupported(relIndex))
			{
				CMDIdGPDB *mdid_index = GPOS_NEW(memory_pool) CMDIdGPDB(index_oid);
				BOOL is_partial = (NULL != logicalIndexInfo->partCons) || (NIL != logicalIndexInfo->defaultLevels);
				CMDIndexInfo *pmdIndexInfo = GPOS_NEW(memory_pool) CMDIndexInfo(mdid_index, is_partial);
				md_index_info_array->Append(pmdIndexInfo);
			}

			gpdb::CloseRelation(relIndex);
		}
		GPOS_CATCH_EX(ex)
		{
			gpdb::CloseRelation(relIndex);
			GPOS_RETHROW(ex);
		}
		GPOS_CATCH_END;
	}
	return md_index_info_array;
}

// return index info list of indexes defined on regular, external tables or leaf partitions
MDIndexInfoPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelIndexInfoForNonPartTable
	(
	IMemoryPool *memory_pool,
	Relation rel
	)
{
	MDIndexInfoPtrArray *md_index_info_array = GPOS_NEW(memory_pool) MDIndexInfoPtrArray(memory_pool);

	// not a partitioned table: obtain indexes directly from the catalog
	List *plIndexOids = gpdb::GetRelationIndexes(rel);

	ListCell *lc = NULL;

	ForEach (lc, plIndexOids)
	{
		OID index_oid = lfirst_oid(lc);

		// only add supported indexes
		Relation relIndex = gpdb::GetRelation(index_oid);

		if (NULL == relIndex)
		{
			WCHAR wsz[1024];
			CWStringStatic str(wsz, 1024);
			COstreamString oss(&str);
			oss << (ULONG) index_oid;
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, str.GetBuffer());
		}

		GPOS_ASSERT(NULL != relIndex->rd_indextuple);

		GPOS_TRY
		{
			if (IsIndexSupported(relIndex))
			{
				CMDIdGPDB *mdid_index = GPOS_NEW(memory_pool) CMDIdGPDB(index_oid);
				// for a regular table, external table or leaf partition, an index is always complete
				CMDIndexInfo *pmdIndexInfo = GPOS_NEW(memory_pool) CMDIndexInfo(mdid_index, false /* is_partial */);
				md_index_info_array->Append(pmdIndexInfo);
			}

			gpdb::CloseRelation(relIndex);
		}
		GPOS_CATCH_EX(ex)
		{
			gpdb::CloseRelation(relIndex);
			GPOS_RETHROW(ex);
		}
		GPOS_CATCH_END;
	}

	return md_index_info_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartTableIndexInfo
//
//	@doc:
//		Return the index info list of on a partitioned table
//
//---------------------------------------------------------------------------
List *
CTranslatorRelcacheToDXL::RetrievePartTableIndexInfo
	(
	Relation rel
	)
{
	List *plgidxinfo = NIL;
	
	LogicalIndexes *plgidx = gpdb::GetLogicalPartIndexes(rel->rd_id);

	if (NULL == plgidx)
	{
		return NIL;
	}
	GPOS_ASSERT(NULL != plgidx);
	GPOS_ASSERT(0 <= plgidx->numLogicalIndexes);
	
	const ULONG ulIndexes = (ULONG) plgidx->numLogicalIndexes;
	for (ULONG ul = 0; ul < ulIndexes; ul++)
	{
		LogicalIndexInfo *pidxinfo = (plgidx->logicalIndexInfo)[ul];
		plgidxinfo = gpdb::LAppend(plgidxinfo, pidxinfo);
	}
	
	gpdb::GPDBFree(plgidx);
	
	return plgidxinfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelTriggers
//
//	@doc:
//		Return the triggers defined on the given relation
//
//---------------------------------------------------------------------------
MdidPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelTriggers
	(
	IMemoryPool *memory_pool,
	Relation rel
	)
{
	GPOS_ASSERT(NULL != rel);
	if (rel->rd_rel->relhastriggers && NULL == rel->trigdesc)
	{
		gpdb::BuildRelationTriggers(rel);
		if (NULL == rel->trigdesc)
		{
			rel->rd_rel->relhastriggers = false;
		}
	}

	MdidPtrArray *mdid_triggers_array = GPOS_NEW(memory_pool) MdidPtrArray(memory_pool);
	if (rel->rd_rel->relhastriggers)
	{
		const ULONG ulTriggers = rel->trigdesc->numtriggers;

		for (ULONG ul = 0; ul < ulTriggers; ul++)
		{
			Trigger trigger = rel->trigdesc->triggers[ul];
			OID oidTrigger = trigger.tgoid;
			CMDIdGPDB *pmdidTrigger = GPOS_NEW(memory_pool) CMDIdGPDB(oidTrigger);
			mdid_triggers_array->Append(pmdidTrigger);
		}
	}

	return mdid_triggers_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelCheckConstraints
//
//	@doc:
//		Return the check constraints defined on the relation with the given oid
//
//---------------------------------------------------------------------------
MdidPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelCheckConstraints
	(
	IMemoryPool *memory_pool,
	OID oid
	)
{
	MdidPtrArray *pdrgpmdidCheckConstraints = GPOS_NEW(memory_pool) MdidPtrArray(memory_pool);
	List *plOidCheckConstraints = gpdb::GetCheckConstraintOids(oid);

	ListCell *plcOid = NULL;
	ForEach (plcOid, plOidCheckConstraints)
	{
		OID oidCheckConstraint = lfirst_oid(plcOid);
		GPOS_ASSERT(0 != oidCheckConstraint);
		CMDIdGPDB *pmdidCheckConstraint = GPOS_NEW(memory_pool) CMDIdGPDB(oidCheckConstraint);
		pdrgpmdidCheckConstraints->Append(pmdidCheckConstraint);
	}

	return pdrgpmdidCheckConstraints;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::CheckUnsupportedRelation
//
//	@doc:
//		Check and fall back to planner for unsupported relations
//
//---------------------------------------------------------------------------
void
CTranslatorRelcacheToDXL::CheckUnsupportedRelation
	(
	OID oidRel
	)
{
	if (gpdb::RelPartIsInterior(oidRel))
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Query on intermediate partition"));
	}

	List *plPartKeys = gpdb::GetPartitionAttrs(oidRel);
	ULONG num_of_levels = gpdb::ListLength(plPartKeys);

	if (0 == num_of_levels && gpdb::HasSubclassSlow(oidRel))
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Inherited tables"));
	}

	if (1 < num_of_levels)
	{
		if (!optimizer_multilevel_partitioning)
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Multi-level partitioned tables"));
		}

		if (!gpdb::IsMultilevelPartitionUniform(oidRel))
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Multi-level partitioned tables with non-uniform partitioning structure"));
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRel
//
//	@doc:
//		Retrieve a relation from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDRelation *
CTranslatorRelcacheToDXL::RetrieveRel
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	OID oid = CMDIdGPDB::CastMdid(mdid)->OidObjectId();
	GPOS_ASSERT(InvalidOid != oid);

	CheckUnsupportedRelation(oid);

	Relation rel = gpdb::GetRelation(oid);

	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	// GPDB_91_MERGE_FIXME - Orca does not support foreign data
	if (RelationIsForeign(rel))
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Foreign Data"));
	}


	CMDName *mdname = NULL;
	IMDRelation::Erelstoragetype rel_storage_type = IMDRelation::ErelstorageSentinel;
	MDColumnPtrArray *mdcol_array = NULL;
	IMDRelation::Ereldistrpolicy ereldistribution = IMDRelation::EreldistrSentinel;
	ULongPtrArray *pdrpulDistrCols = NULL;
	MDIndexInfoPtrArray *md_index_info_array = NULL;
	MdidPtrArray *mdid_triggers_array = NULL;
	ULongPtrArray *pdrgpulPartKeys = NULL;
	CharPtrArray *pdrgpszPartTypes = NULL;
	ULONG ulLeafPartitions = 0;
	BOOL convert_hash_to_random = false;
	ULongPtrArray2D *keyset_array = NULL;
	MdidPtrArray *pdrgpmdidCheckConstraints = NULL;
	BOOL fTemporary = false;
	BOOL has_oids = false;
	BOOL fPartitioned = false;
	IMDRelation *md_rel = NULL;


	GPOS_TRY
	{
		// get rel name
		mdname = GetRelName(memory_pool, rel);

		// get storage type
		rel_storage_type = RetrieveRelStorageType(rel->rd_rel->relstorage);

		// get relation columns
		mdcol_array = RetrieveRelColumns(memory_pool, md_accessor, rel, rel_storage_type);
		const ULONG ulMaxCols = GPDXL_SYSTEM_COLUMNS + (ULONG) rel->rd_att->natts + 1;
		ULONG *pulAttnoMapping = ConstructAttnoMapping(memory_pool, mdcol_array, ulMaxCols);

		// get distribution policy
		GpPolicy *pgppolicy = gpdb::GetDistributionPolicy(rel);
		ereldistribution = GetRelDistribution(pgppolicy);

		// get distribution columns
		if (IMDRelation::EreldistrHash == ereldistribution)
		{
			pdrpulDistrCols = RetrieveRelDistrbutionCols(memory_pool, pgppolicy, mdcol_array, ulMaxCols);
		}

		convert_hash_to_random = gpdb::IsChildPartDistributionMismatched(rel);

		// collect relation indexes
		md_index_info_array = RetrieveRelIndexInfo(memory_pool, rel);

		// collect relation triggers
		mdid_triggers_array = RetrieveRelTriggers(memory_pool, rel);

		// get partition keys
		if (IMDRelation::ErelstorageExternal != rel_storage_type)
		{
			RetrievePartKeysAndTypes(memory_pool, rel, oid, &pdrgpulPartKeys, &pdrgpszPartTypes);
		}
		fPartitioned = (NULL != pdrgpulPartKeys && 0 < pdrgpulPartKeys->Size());

		if (fPartitioned && IMDRelation::ErelstorageAppendOnlyParquet != rel_storage_type && IMDRelation::ErelstorageExternal != rel_storage_type)
		{
			// mark relation as Parquet if one of its children is parquet
			if (gpdb::HasParquetChildren(oid))
			{
				rel_storage_type = IMDRelation::ErelstorageAppendOnlyParquet;
			}
		}

		// get number of leaf partitions
		if (gpdb::RelPartIsRoot(oid))
		{
			ulLeafPartitions = gpdb::CountLeafPartTables(oid);
		}

		// get key sets
		BOOL fAddDefaultKeys = RelHasSystemColumns(rel->rd_rel->relkind);
		keyset_array = RetrieveRelKeysets(memory_pool, oid, fAddDefaultKeys, fPartitioned, pulAttnoMapping);

		// collect all check constraints
		pdrgpmdidCheckConstraints = RetrieveRelCheckConstraints(memory_pool, oid);

		fTemporary = (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP);
		has_oids = rel->rd_rel->relhasoids;
	
		GPOS_DELETE_ARRAY(pulAttnoMapping);
		gpdb::CloseRelation(rel);
	}
	GPOS_CATCH_EX(ex)
	{
		gpdb::CloseRelation(rel);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	GPOS_ASSERT(IMDRelation::ErelstorageSentinel != rel_storage_type);
	GPOS_ASSERT(IMDRelation::EreldistrSentinel != ereldistribution);

	mdid->AddRef();

	if (IMDRelation::ErelstorageExternal == rel_storage_type)
	{
		ExtTableEntry *extentry = gpdb::GetExtTableEntry(oid);

		md_rel = GPOS_NEW(memory_pool) CMDRelationExternalGPDB
							(
							memory_pool,
							mdid,
							mdname,
							ereldistribution,
							mdcol_array,
							pdrpulDistrCols,
							convert_hash_to_random,
							keyset_array,
							md_index_info_array,
							mdid_triggers_array,
							pdrgpmdidCheckConstraints,
							extentry->rejectlimit,
							('r' == extentry->rejectlimittype),
							NULL /* it's sufficient to pass NULL here since ORCA
								doesn't really make use of the logerrors value.
								In case of converting the DXL returned from to
								PlanStmt, currently the code looks up the information
								from catalog and fill in the required values into the ExternalScan */
							);
	}
	else
	{
		CMDPartConstraintGPDB *mdpart_constraint = NULL;

		// retrieve the part constraints if relation is partitioned
		if (fPartitioned)
			mdpart_constraint = RetrievePartConstraintForRel(memory_pool, md_accessor, oid, mdcol_array, md_index_info_array->Size() > 0 /*fhasIndex*/);

		md_rel = GPOS_NEW(memory_pool) CMDRelationGPDB
							(
							memory_pool,
							mdid,
							mdname,
							fTemporary,
							rel_storage_type,
							ereldistribution,
							mdcol_array,
							pdrpulDistrCols,
							pdrgpulPartKeys,
							pdrgpszPartTypes,
							ulLeafPartitions,
							convert_hash_to_random,
							keyset_array,
							md_index_info_array,
							mdid_triggers_array,
							pdrgpmdidCheckConstraints,
							mdpart_constraint,
							has_oids
							);
	}

	return md_rel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelColumns
//
//	@doc:
//		Get relation columns
//
//---------------------------------------------------------------------------
MDColumnPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelColumns
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	Relation rel,
	IMDRelation::Erelstoragetype rel_storage_type
	)
{
	MDColumnPtrArray *mdcol_array = GPOS_NEW(memory_pool) MDColumnPtrArray(memory_pool);

	for (ULONG ul = 0;  ul < (ULONG) rel->rd_att->natts; ul++)
	{
		Form_pg_attribute att = rel->rd_att->attrs[ul];
		CMDName *md_colname = CDXLUtils::CreateMDNameFromCharArray(memory_pool, NameStr(att->attname));
	
		// translate the default column value
		CDXLNode *pdxlnDefault = NULL;
		
		if (!att->attisdropped)
		{
			pdxlnDefault = GetDefaultColumnValue(memory_pool, md_accessor, rel->rd_att, att->attnum);
		}

		ULONG ulColLen = gpos::ulong_max;
		CMDIdGPDB *pmdidCol = GPOS_NEW(memory_pool) CMDIdGPDB(att->atttypid);
		HeapTuple heaptupleStats = gpdb::GetAttStats(rel->rd_id, ul+1);

		// Column width priority:
		// 1. If there is average width kept in the stats for that column, pick that value.
		// 2. If not, if it is a fixed length text type, pick the size of it. E.g if it is
		//    varchar(10), assign 10 as the column length.
		// 3. Else if it not dropped and a fixed length type such as int4, assign the fixed
		//    length.
		// 4. Otherwise, assign it to default column width which is 8.
		if(HeapTupleIsValid(heaptupleStats))
		{
			Form_pg_statistic fpsStats = (Form_pg_statistic) GETSTRUCT(heaptupleStats);

			// column width
			ulColLen = fpsStats->stawidth;
			gpdb::FreeHeapTuple(heaptupleStats);
		}
		else if ((pmdidCol->Equals(&CMDIdGPDB::m_mdid_bpchar) || pmdidCol->Equals(&CMDIdGPDB::m_mdid_varchar)) && (VARHDRSZ < att->atttypmod))
		{
			ulColLen = (ULONG) att->atttypmod - VARHDRSZ;
		}
		else
		{
			DOUBLE width = CStatistics::DefaultColumnWidth.Get();
			ulColLen = (ULONG) width;

			if (!att->attisdropped)
			{
				IMDType *md_type = CTranslatorRelcacheToDXL::RetrieveType(memory_pool, pmdidCol);
				if(md_type->IsFixedLength())
				{
					ulColLen = md_type->Length();
				}
				md_type->Release();
			}
		}

		CMDColumn *md_col = GPOS_NEW(memory_pool) CMDColumn
										(
										md_colname,
										att->attnum,
										pmdidCol,
										att->atttypmod,
										!att->attnotnull,
										att->attisdropped,
										pdxlnDefault /* default value */,
										ulColLen
										);

		mdcol_array->Append(md_col);
	}

	// add system columns
	if (RelHasSystemColumns(rel->rd_rel->relkind))
	{
		BOOL fAOTable = IMDRelation::ErelstorageAppendOnlyRows == rel_storage_type ||
				IMDRelation::ErelstorageAppendOnlyCols == rel_storage_type;
		AddSystemColumns(memory_pool, mdcol_array, rel, fAOTable);
	}

	return mdcol_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetDefaultColumnValue
//
//	@doc:
//		Return the dxl representation of column's default value
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorRelcacheToDXL::GetDefaultColumnValue
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	TupleDesc rd_att,
	AttrNumber attno
	)
{
	GPOS_ASSERT(attno > 0);

	Node *node = NULL;

	// Scan to see if relation has a default for this column
	if (NULL != rd_att->constr && 0 < rd_att->constr->num_defval)
	{
		AttrDefault *defval = rd_att->constr->defval;
		INT	iNumDef = rd_att->constr->num_defval;

		GPOS_ASSERT(NULL != defval);
		for (ULONG ulCounter = 0; ulCounter < (ULONG) iNumDef; ulCounter++)
		{
			if (attno == defval[ulCounter].adnum)
			{
				// found it, convert string representation to node tree.
				node = gpdb::StringToNode(defval[ulCounter].adbin);
				break;
			}
		}
	}

	if (NULL == node)
	{
		// get the default value for the type
		Form_pg_attribute att_tup = rd_att->attrs[attno - 1];
		Oid	oidAtttype = att_tup->atttypid;
		node = gpdb::GetTypeDefault(oidAtttype);
	}

	if (NULL == node)
	{
		return NULL;
	}

	// translate the default value expression
	CTranslatorScalarToDXL sctranslator
							(
							memory_pool,
							md_accessor,
							NULL, /* pulidgtorCol */
							NULL, /* pulidgtorCTE */
							0, /* query_level */
							true, /* m_fQuery */
							NULL, /* query_level_to_cte_map */
							NULL /* cte_dxlnode_array */
							);

	return sctranslator.CreateScalarOpFromExpr
							(
							(Expr *) node,
							NULL /* var_col_id_mapping --- subquery or external variable are not supported in default expression */
							);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetRelDistribution
//
//	@doc:
//		Return the distribution policy of the relation
//
//---------------------------------------------------------------------------
IMDRelation::Ereldistrpolicy
CTranslatorRelcacheToDXL::GetRelDistribution
	(
	GpPolicy *pgppolicy
	)
{
	if (NULL == pgppolicy)
	{
		return IMDRelation::EreldistrMasterOnly;
	}

	if (POLICYTYPE_PARTITIONED == pgppolicy->ptype)
	{
		if (0 == pgppolicy->nattrs)
		{
			return IMDRelation::EreldistrRandom;
		}

		return IMDRelation::EreldistrHash;
	}

	if (POLICYTYPE_ENTRY == pgppolicy->ptype)
	{
		return IMDRelation::EreldistrMasterOnly;
	}

	GPOS_RAISE(gpdxl::ExmaMD, ExmiDXLUnrecognizedType, GPOS_WSZ_LIT("unrecognized distribution policy"));
	return IMDRelation::EreldistrSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelDistrbutionCols
//
//	@doc:
//		Get distribution columns
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorRelcacheToDXL::RetrieveRelDistrbutionCols
	(
	IMemoryPool *memory_pool,
	GpPolicy *pgppolicy,
	MDColumnPtrArray *mdcol_array,
	ULONG size
	)
{
	ULONG *pul = GPOS_NEW_ARRAY(memory_pool , ULONG, size);

	for (ULONG ul = 0;  ul < mdcol_array->Size(); ul++)
	{
		const IMDColumn *md_col = (*mdcol_array)[ul];
		INT attno = md_col->AttrNum();

		ULONG ulIndex = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
		pul[ulIndex] = ul;
	}

	ULongPtrArray *pdrpulDistrCols = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);

	for (ULONG ul = 0; ul < (ULONG) pgppolicy->nattrs; ul++)
	{
		AttrNumber attno = pgppolicy->attrs[ul];
		pdrpulDistrCols->Append(GPOS_NEW(memory_pool) ULONG(GetAttributePosition(attno, pul)));
	}

	GPOS_DELETE_ARRAY(pul);
	return pdrpulDistrCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::AddSystemColumns
//
//	@doc:
//		Adding system columns (oid, tid, xmin, etc) in table descriptors
//
//---------------------------------------------------------------------------
void
CTranslatorRelcacheToDXL::AddSystemColumns
	(
	IMemoryPool *memory_pool,
	MDColumnPtrArray *mdcol_array,
	Relation rel,
	BOOL fAOTable
	)
{
	BOOL fHasOid = rel->rd_att->tdhasoid;
	fAOTable = fAOTable || gpdb::IsAppendOnlyPartitionTable(rel->rd_id);

	for (INT i= SelfItemPointerAttributeNumber; i > FirstLowInvalidHeapAttributeNumber; i--)
	{
		AttrNumber attno = AttrNumber(i);
		GPOS_ASSERT(0 != attno);

		if (ObjectIdAttributeNumber == i && !fHasOid)
		{
			continue;
		}

		if (IsTransactionVisibilityAttribute(i) && fAOTable)
		{
			// skip transaction attrbutes like xmin, xmax, cmin, cmax for AO tables
			continue;
		}

		// get system name for that attribute
		const CWStringConst *pstrSysColName = CTranslatorUtils::GetSystemColName(attno);
		GPOS_ASSERT(NULL != pstrSysColName);

		// copy string into column name
		CMDName *md_colname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrSysColName);

		CMDColumn *md_col = GPOS_NEW(memory_pool) CMDColumn
										(
										md_colname, 
										attno, 
										CTranslatorUtils::GetSystemColType(memory_pool, attno),
										default_type_modifier,
										false,	// is_nullable
										false,	// is_dropped
										NULL,	// default value
										CTranslatorUtils::GetSystemColLength(attno)
										);

		mdcol_array->Append(md_col);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::IsTransactionVisibilityAttribute
//
//	@doc:
//		Check if attribute number is one of the system attributes related to 
//		transaction visibility such as xmin, xmax, cmin, cmax
//
//---------------------------------------------------------------------------
BOOL
CTranslatorRelcacheToDXL::IsTransactionVisibilityAttribute
	(
	INT attrnum
	)
{
	return attrnum == MinTransactionIdAttributeNumber || attrnum == MaxTransactionIdAttributeNumber || 
			attrnum == MinCommandIdAttributeNumber || attrnum == MaxCommandIdAttributeNumber;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveIndex
//
//	@doc:
//		Retrieve an index from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDIndex *
CTranslatorRelcacheToDXL::RetrieveIndex
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid_index
	)
{
	OID index_oid = CMDIdGPDB::CastMdid(mdid_index)->OidObjectId();
	GPOS_ASSERT(0 != index_oid);
	Relation relIndex = gpdb::GetRelation(index_oid);

	if (NULL == relIndex)
	{
		 GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid_index->GetBuffer());
	}

	const IMDRelation *md_rel = NULL;
	Form_pg_index pgIndex = NULL;
	CMDName *mdname = NULL;
	IMDIndex::EmdindexType emdindt = IMDIndex::EmdindSentinel;
	IMDId *mdid_item_type = NULL;

	GPOS_TRY
	{
		if (!IsIndexSupported(relIndex))
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Index type"));
		}

		pgIndex = relIndex->rd_index;
		GPOS_ASSERT (NULL != pgIndex);

		OID oidRel = pgIndex->indrelid;

		if (gpdb::IsLeafPartition(oidRel))
		{
			oidRel = gpdb::GetRootPartition(oidRel);
		}

		CMDIdGPDB *pmdidRel = GPOS_NEW(memory_pool) CMDIdGPDB(oidRel);

		md_rel = md_accessor->RetrieveRel(pmdidRel);
	
		if (md_rel->IsPartitioned())
		{
			LogicalIndexes *plgidx = gpdb::GetLogicalPartIndexes(oidRel);
			GPOS_ASSERT(NULL != plgidx);

			IMDIndex *index = RetrievePartTableIndex(memory_pool, md_accessor, mdid_index, md_rel, plgidx);

			// cleanup
			gpdb::GPDBFree(plgidx);

			if (NULL != index)
			{
				pmdidRel->Release();
				gpdb::CloseRelation(relIndex);
				return index;
			}
		}
	
		emdindt = IMDIndex::EmdindBtree;
		IMDRelation::Erelstoragetype rel_storage_type = md_rel->RetrieveRelStorageType();
		if (BITMAP_AM_OID == relIndex->rd_rel->relam || IMDRelation::ErelstorageAppendOnlyRows == rel_storage_type || IMDRelation::ErelstorageAppendOnlyCols == rel_storage_type)
		{
			emdindt = IMDIndex::EmdindBitmap;
			mdid_item_type = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_ANY);
		}
		
		// get the index name
		CHAR *szIndexName = NameStr(relIndex->rd_rel->relname);
		CWStringDynamic *str_name = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szIndexName);
		mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, str_name);
		GPOS_DELETE(str_name);
		pmdidRel->Release();
		gpdb::CloseRelation(relIndex);
	}
	GPOS_CATCH_EX(ex)
	{
		gpdb::CloseRelation(relIndex);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
	
	Relation relTable = gpdb::GetRelation(CMDIdGPDB::CastMdid(md_rel->MDId())->OidObjectId());
	ULONG ulRgSize = GPDXL_SYSTEM_COLUMNS + (ULONG) relTable->rd_att->natts + 1;
	gpdb::CloseRelation(relTable); // close relation as early as possible

	ULONG *pul = PopulateAttnoPositionMap(memory_pool, md_rel, ulRgSize);

	ULongPtrArray *pdrgpulIncludeCols = ComputeIncludedCols(memory_pool, md_rel);

	// extract the position of the key columns
	ULongPtrArray *index_key_cols_array = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	ULONG ulKeys = pgIndex->indnatts;
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		INT attno = pgIndex->indkey.values[ul];
		GPOS_ASSERT(0 != attno && "Index expressions not supported");

		index_key_cols_array->Append(GPOS_NEW(memory_pool) ULONG(GetAttributePosition(attno, pul)));
	}

	mdid_index->AddRef();	
	MdidPtrArray *pdrgpmdidOpFamilies = RetrieveIndexOpFamilies(memory_pool, mdid_index);
	
	CMDIndexGPDB *index = GPOS_NEW(memory_pool) CMDIndexGPDB
										(
										memory_pool,
										mdid_index,
										mdname,
										pgIndex->indisclustered,
										emdindt,
										mdid_item_type,
										index_key_cols_array,
										pdrgpulIncludeCols,
										pdrgpmdidOpFamilies,
										NULL // mdpart_constraint
										);

	GPOS_DELETE_ARRAY(pul);

	return index;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartTableIndex
//
//	@doc:
//		Retrieve an index over a partitioned table from the relcache given its 
//		mdid
//
//---------------------------------------------------------------------------
IMDIndex *
CTranslatorRelcacheToDXL::RetrievePartTableIndex
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid_index,
	const IMDRelation *md_rel,
	LogicalIndexes *plind
	)
{
	GPOS_ASSERT(NULL != plind);
	GPOS_ASSERT(0 < plind->numLogicalIndexes);
	
	OID oid = CMDIdGPDB::CastMdid(mdid_index)->OidObjectId();
	
	LogicalIndexInfo *pidxinfo = LookupLogicalIndexById(plind, oid);
	if (NULL == pidxinfo)
	{
		 return NULL;
	}
	
	return RetrievePartTableIndex(memory_pool, md_accessor, pidxinfo, mdid_index, md_rel);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::LookupLogicalIndexById
//
//	@doc:
//		Lookup an index given its id from the logical indexes structure
//
//---------------------------------------------------------------------------
LogicalIndexInfo *
CTranslatorRelcacheToDXL::LookupLogicalIndexById
	(
	LogicalIndexes *plind, 
	OID oid
	)
{
	GPOS_ASSERT(NULL != plind && 0 <= plind->numLogicalIndexes);
	
	const ULONG ulIndexes = plind->numLogicalIndexes;
	
	for (ULONG ul = 0; ul < ulIndexes; ul++)
	{
		LogicalIndexInfo *pidxinfo = (plind->logicalIndexInfo)[ul];
		
		if (oid == pidxinfo->logicalIndexOid)
		{
			return pidxinfo;
		}
	}
	
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartTableIndex
//
//	@doc:
//		Construct an MD cache index object given its logical index representation
//
//---------------------------------------------------------------------------
IMDIndex *
CTranslatorRelcacheToDXL::RetrievePartTableIndex
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	LogicalIndexInfo *pidxinfo,
	IMDId *mdid_index,
	const IMDRelation *md_rel
	)
{
	OID index_oid = pidxinfo->logicalIndexOid;
	
	Relation relIndex = gpdb::GetRelation(index_oid);

	if (NULL == relIndex)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid_index->GetBuffer());
	}

	if (!IsIndexSupported(relIndex))
	{
		gpdb::CloseRelation(relIndex);
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Index type"));
	}
	
	// get the index name
	GPOS_ASSERT(NULL != relIndex->rd_index);
	Form_pg_index pgIndex = relIndex->rd_index;
	
	CHAR *szIndexName = NameStr(relIndex->rd_rel->relname);
	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(memory_pool, szIndexName);
	gpdb::CloseRelation(relIndex);

	OID oidRel = CMDIdGPDB::CastMdid(md_rel->MDId())->OidObjectId();
	Relation relTable = gpdb::GetRelation(oidRel);
	ULONG ulRgSize = GPDXL_SYSTEM_COLUMNS + (ULONG) relTable->rd_att->natts + 1;
	gpdb::CloseRelation(relTable);

	ULONG *pulAttrMap = PopulateAttnoPositionMap(memory_pool, md_rel, ulRgSize);

	ULongPtrArray *pdrgpulIncludeCols = ComputeIncludedCols(memory_pool, md_rel);

	// extract the position of the key columns
	ULongPtrArray *index_key_cols_array = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	const ULONG ulKeys = pidxinfo->nColumns;
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		INT attno = pidxinfo->indexKeys[ul];
		GPOS_ASSERT(0 != attno && "Index expressions not supported");

		index_key_cols_array->Append(GPOS_NEW(memory_pool) ULONG(GetAttributePosition(attno, pulAttrMap)));
	}
	
	/*
	 * If an index exists only on a leaf part, pnodePartCnstr refers to the expression
	 * identifying the path to reach the partition holding the index. For indexes
	 * available on all parts it is set to NULL.
	 */
	Node *pnodePartCnstr = pidxinfo->partCons;
	
	/*
	 * If an index exists all on the parts including default, the logical index
	 * info created marks defaultLevels as NIL. However, if an index exists only on
	 * leaf parts plDefaultLevel contains the default part level which come across while
	 * reaching to the leaf part from root.
	 */
	List *plDefaultLevels = pidxinfo->defaultLevels;
	
	// get number of partitioning levels
	List *plPartKeys = gpdb::GetPartitionAttrs(oidRel);
	const ULONG num_of_levels = gpdb::ListLength(plPartKeys);
	gpdb::ListFree(plPartKeys);

	/* get relation constraints
	 * plDefaultLevelsRel indicates the levels on which default partitions exists
	 * for the partitioned table
	 */
	List *plDefaultLevelsRel = NIL;
	Node *pnodePartCnstrRel = gpdb::GetRelationPartContraints(oidRel, &plDefaultLevelsRel);

	BOOL is_unbounded = (NULL == pnodePartCnstr) && (NIL == plDefaultLevels);
	for (ULONG ul = 0; ul < num_of_levels; ul++)
	{
		is_unbounded = is_unbounded && IsDefaultPartition(plDefaultLevelsRel, ul);
	}

	/*
	 * If pnodePartCnstr is NULL and plDefaultLevels is NIL,
	 * it indicates that the index is available on all the parts including
	 * default part. So, we can say that levels on which default partitions
	 * exists for the relation applies to the index as well and the relative
	 * scan will not be partial.
	 */
	List *plDefaultLevelsDerived = NIL;
	if (NULL == pnodePartCnstr && NIL == plDefaultLevels)
		plDefaultLevelsDerived = plDefaultLevelsRel;
	else
		plDefaultLevelsDerived = plDefaultLevels;
	
	ULongPtrArray *pdrgpulDefaultLevels = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	for (ULONG ul = 0; ul < num_of_levels; ul++)
	{
		if (is_unbounded || IsDefaultPartition(plDefaultLevelsDerived, ul))
		{
			pdrgpulDefaultLevels->Append(GPOS_NEW(memory_pool) ULONG(ul));
		}
	}
	gpdb::ListFree(plDefaultLevelsDerived);

	if (NULL == pnodePartCnstr)
	{
		if (NIL == plDefaultLevels)
		{
			// NULL part constraints means all non-default partitions -> get constraint from the part table
			pnodePartCnstr = pnodePartCnstrRel;
		}
		else
		{
			pnodePartCnstr = gpdb::MakeBoolConst(false /*value*/, false /*isull*/);
		}
	}
		
	CMDPartConstraintGPDB *mdpart_constraint = RetrievePartConstraintForIndex(memory_pool, md_accessor, md_rel, pnodePartCnstr, pdrgpulDefaultLevels, is_unbounded);

	pdrgpulDefaultLevels->Release();
	mdid_index->AddRef();
	
	GPOS_ASSERT(INDTYPE_BITMAP == pidxinfo->indType || INDTYPE_BTREE == pidxinfo->indType);
	
	IMDIndex::EmdindexType emdindt = IMDIndex::EmdindBtree;
	IMDId *mdid_item_type = NULL;
	if (INDTYPE_BITMAP == pidxinfo->indType)
	{
		emdindt = IMDIndex::EmdindBitmap;
		mdid_item_type = GPOS_NEW(memory_pool) CMDIdGPDB(GPDB_ANY);
	}
	
	MdidPtrArray *pdrgpmdidOpFamilies = RetrieveIndexOpFamilies(memory_pool, mdid_index);
	
	CMDIndexGPDB *index = GPOS_NEW(memory_pool) CMDIndexGPDB
										(
										memory_pool,
										mdid_index,
										mdname,
										pgIndex->indisclustered,
										emdindt,
										mdid_item_type,
										index_key_cols_array,
										pdrgpulIncludeCols,
										pdrgpmdidOpFamilies,
										mdpart_constraint
										);
	
	GPOS_DELETE_ARRAY(pulAttrMap);
	
	return index;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::IsDefaultPartition
//
//	@doc:
//		Check whether the default partition at level one is included
//
//---------------------------------------------------------------------------
BOOL
CTranslatorRelcacheToDXL::IsDefaultPartition
	(
	List *plDefaultLevels,
	ULONG ulLevel
	)
{
	if (NIL == plDefaultLevels)
	{
		return false;
	}
	
	ListCell *lc = NULL;
	ForEach (lc, plDefaultLevels)
	{
		ULONG ulDefaultLevel = (ULONG) lfirst_int(lc);
		if (ulLevel == ulDefaultLevel)
		{
			return true;
		}
	}
	
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::ComputeIncludedCols
//
//	@doc:
//		Compute the included columns in an index
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorRelcacheToDXL::ComputeIncludedCols
	(
	IMemoryPool *memory_pool,
	const IMDRelation *md_rel
	)
{
	// TODO: 3/19/2012; currently we assume that all the columns
	// in the table are available from the index.

	ULongPtrArray *pdrgpulIncludeCols = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	const ULONG ulIncludedCols = md_rel->ColumnCount();
	for (ULONG ul = 0;  ul < ulIncludedCols; ul++)
	{
		if (!md_rel->GetMdCol(ul)->IsDropped())
		{
			pdrgpulIncludeCols->Append(GPOS_NEW(memory_pool) ULONG(ul));
		}
	}
	
	return pdrgpulIncludeCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetAttributePosition
//
//	@doc:
//		Return the position of a given attribute
//
//---------------------------------------------------------------------------
ULONG
CTranslatorRelcacheToDXL::GetAttributePosition
	(
	INT attno,
	ULONG *pul
	)
{
	ULONG ulIndex = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
	ULONG ulPos = pul[ulIndex];
	GPOS_ASSERT(gpos::ulong_max != ulPos);

	return ulPos;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::PopulateAttnoPositionMap
//
//	@doc:
//		Populate the attribute to position mapping
//
//---------------------------------------------------------------------------
ULONG *
CTranslatorRelcacheToDXL::PopulateAttnoPositionMap
	(
	IMemoryPool *memory_pool,
	const IMDRelation *md_rel,
	ULONG size
	)
{
	GPOS_ASSERT(NULL != md_rel);
	const ULONG ulIncludedCols = md_rel->ColumnCount();

	GPOS_ASSERT(ulIncludedCols <= size);
	ULONG *pul = GPOS_NEW_ARRAY(memory_pool , ULONG, size);

	for (ULONG ul = 0; ul < size; ul++)
	{
		pul[ul] = gpos::ulong_max;
	}

	for (ULONG ul = 0;  ul < ulIncludedCols; ul++)
	{
		const IMDColumn *md_col = md_rel->GetMdCol(ul);

		INT attno = md_col->AttrNum();

		ULONG ulIndex = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
		GPOS_ASSERT(size > ulIndex);
		pul[ulIndex] = ul;
	}

	return pul;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveType
//
//	@doc:
//		Retrieve a type from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
IMDType *
CTranslatorRelcacheToDXL::RetrieveType
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->OidObjectId();
	GPOS_ASSERT(InvalidOid != oid_type);
	
	// check for supported base types
	switch (oid_type)
	{
		case GPDB_INT2_OID:
			return GPOS_NEW(memory_pool) CMDTypeInt2GPDB(memory_pool);

		case GPDB_INT4_OID:
			return GPOS_NEW(memory_pool) CMDTypeInt4GPDB(memory_pool);

		case GPDB_INT8_OID:
			return GPOS_NEW(memory_pool) CMDTypeInt8GPDB(memory_pool);

		case GPDB_BOOL:
			return GPOS_NEW(memory_pool) CMDTypeBoolGPDB(memory_pool);

		case GPDB_OID_OID:
			return GPOS_NEW(memory_pool) CMDTypeOidGPDB(memory_pool);
	}

	// continue to construct a generic type
	INT iFlags = TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
				 TYPECACHE_CMP_PROC | TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO | TYPECACHE_TUPDESC;

	TypeCacheEntry *ptce = gpdb::LookupTypeCache(oid_type, iFlags);

	// get type name
	CMDName *mdname = GetTypeName(memory_pool, mdid);

	BOOL is_fixed_length = false;
	ULONG length = 0;

	if (0 < ptce->typlen)
	{
		is_fixed_length = true;
		length = ptce->typlen;
	}

	BOOL is_passed_by_value = ptce->typbyval;

	// collect ids of different comparison operators for types
	CMDIdGPDB *mdid_op_eq = GPOS_NEW(memory_pool) CMDIdGPDB(ptce->eq_opr);
	CMDIdGPDB *mdid_op_neq = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetInverseOp(ptce->eq_opr));
	CMDIdGPDB *mdid_op_lt = GPOS_NEW(memory_pool) CMDIdGPDB(ptce->lt_opr);
	CMDIdGPDB *mdid_op_leq = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetInverseOp(ptce->gt_opr));
	CMDIdGPDB *mdid_op_gt = GPOS_NEW(memory_pool) CMDIdGPDB(ptce->gt_opr);
	CMDIdGPDB *mdid_op_geq = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetInverseOp(ptce->lt_opr));
	CMDIdGPDB *mdid_op_cmp = GPOS_NEW(memory_pool) CMDIdGPDB(ptce->cmp_proc);
	BOOL is_hashable = gpdb::IsOpHashJoinable(ptce->eq_opr, oid_type);
	BOOL is_composite_type = gpdb::IsCompositeType(oid_type);

	// get standard aggregates
	CMDIdGPDB *pmdidMin = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetAggregate("min", oid_type));
	CMDIdGPDB *pmdidMax = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetAggregate("max", oid_type));
	CMDIdGPDB *pmdidAvg = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetAggregate("avg", oid_type));
	CMDIdGPDB *pmdidSum = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetAggregate("sum", oid_type));
	
	// count aggregate is the same for all types
	CMDIdGPDB *pmdidCount = GPOS_NEW(memory_pool) CMDIdGPDB(COUNT_ANY_OID);
	
	// check if type is composite
	CMDIdGPDB *pmdidTypeRelid = NULL;
	if (is_composite_type)
	{
		pmdidTypeRelid = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetTypeRelid(oid_type));
	}

	// get array type mdid
	CMDIdGPDB *mdid_type_array = GPOS_NEW(memory_pool) CMDIdGPDB(gpdb::GetArrayType(oid_type));

	BOOL is_redistributable = gpdb::IsGreenplumDbHashable(oid_type);

	mdid->AddRef();

	return GPOS_NEW(memory_pool) CMDTypeGenericGPDB
						 (
						 memory_pool,
						 mdid,
						 mdname,
						 is_redistributable,
						 is_fixed_length,
						 length,
						 is_passed_by_value,
						 mdid_op_eq,
						 mdid_op_neq,
						 mdid_op_lt,
						 mdid_op_leq,
						 mdid_op_gt,
						 mdid_op_geq,
						 mdid_op_cmp,
						 pmdidMin,
						 pmdidMax,
						 pmdidAvg,
						 pmdidSum,
						 pmdidCount,
						 is_hashable,
						 is_composite_type,
						 pmdidTypeRelid,
						 mdid_type_array,
						 ptce->typlen
						 );
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveScOp
//
//	@doc:
//		Retrieve a scalar operator from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDScalarOpGPDB *
CTranslatorRelcacheToDXL::RetrieveScOp
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oidOp = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidOp);

	// get operator name
	CHAR *szName = gpdb::GetOpName(oidOp);

	if (NULL == szName)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(memory_pool, szName);
	
	OID oidLeft = InvalidOid;
	OID oidRight = InvalidOid;

	// get operator argument types
	gpdb::GetOpInputTypes(oidOp, &oidLeft, &oidRight);

	CMDIdGPDB *mdid_type_left = NULL;
	CMDIdGPDB *mdid_type_right = NULL;

	if (InvalidOid != oidLeft)
	{
		mdid_type_left = GPOS_NEW(memory_pool) CMDIdGPDB(oidLeft);
	}

	if (InvalidOid != oidRight)
	{
		mdid_type_right = GPOS_NEW(memory_pool) CMDIdGPDB(oidRight);
	}

	// get comparison type
	CmpType cmpt = (CmpType) gpdb::GetComparisonType(oidOp, oidLeft, oidRight);
	IMDType::ECmpType ecmpt = ParseCmpType(cmpt);
	
	// get func oid
	OID oidFunc = gpdb::GetOpFunc(oidOp);
	GPOS_ASSERT(InvalidOid != oidFunc);

	CMDIdGPDB *mdid_func = GPOS_NEW(memory_pool) CMDIdGPDB(oidFunc);

	// get result type
	OID oidResult = gpdb::GetFuncRetType(oidFunc);

	GPOS_ASSERT(InvalidOid != oidResult);

	CMDIdGPDB *result_type_mdid = GPOS_NEW(memory_pool) CMDIdGPDB(oidResult);

	// get commutator and inverse
	CMDIdGPDB *mdid_commute_opr = NULL;

	OID oidCommute = gpdb::GetCommutatorOp(oidOp);

	if(InvalidOid != oidCommute)
	{
		mdid_commute_opr = GPOS_NEW(memory_pool) CMDIdGPDB(oidCommute);
	}

	CMDIdGPDB *m_mdid_inverse_opr = NULL;

	OID oidInverse = gpdb::GetInverseOp(oidOp);

	if(InvalidOid != oidInverse)
	{
		m_mdid_inverse_opr = GPOS_NEW(memory_pool) CMDIdGPDB(oidInverse);
	}

	BOOL returns_null_on_null_input = gpdb::IsOpStrict(oidOp);

	mdid->AddRef();
	CMDScalarOpGPDB *md_scalar_op = GPOS_NEW(memory_pool) CMDScalarOpGPDB
											(
											memory_pool,
											mdid,
											mdname,
											mdid_type_left,
											mdid_type_right,
											result_type_mdid,
											mdid_func,
											mdid_commute_opr,
											m_mdid_inverse_opr,
											ecmpt,
											returns_null_on_null_input,
											RetrieveScOpOpFamilies(memory_pool, mdid)
											);
	return md_scalar_op;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::LookupFuncProps
//
//	@doc:
//		Lookup function properties
//
//---------------------------------------------------------------------------
void
CTranslatorRelcacheToDXL::LookupFuncProps
	(
	OID oidFunc,
	IMDFunction::EFuncStbl *stability, // output: function stability
	IMDFunction::EFuncDataAcc *access, // output: function datya access
	BOOL *is_strict, // output: is function strict?
	BOOL *ReturnsSet // output: does function return set?
	)
{
	GPOS_ASSERT(NULL != stability);
	GPOS_ASSERT(NULL != access);
	GPOS_ASSERT(NULL != is_strict);
	GPOS_ASSERT(NULL != ReturnsSet);

	CHAR cFuncStability = gpdb::FuncStability(oidFunc);
	*stability = GetFuncStability(cFuncStability);

	CHAR cFuncDataAccess = gpdb::FuncDataAccess(oidFunc);
	*access = GetEFuncDataAccess(cFuncDataAccess);

	CHAR cFuncExecLocation = gpdb::FuncExecLocation(oidFunc);
	if (cFuncExecLocation != PROEXECLOCATION_ANY)
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("unsupported exec location"));

	*ReturnsSet = gpdb::GetFuncRetset(oidFunc);
	*is_strict = gpdb::FuncStrict(oidFunc);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveFunc
//
//	@doc:
//		Retrieve a function from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDFunctionGPDB *
CTranslatorRelcacheToDXL::RetrieveFunc
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oidFunc = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidFunc);

	// get func name
	CHAR *szName = gpdb::GetFuncName(oidFunc);

	if (NULL == szName)
	{

		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CWStringDynamic *pstrFuncName = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szName);
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrFuncName);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(pstrFuncName);

	// get result type
	OID oidResult = gpdb::GetFuncRetType(oidFunc);

	GPOS_ASSERT(InvalidOid != oidResult);

	CMDIdGPDB *result_type_mdid = GPOS_NEW(memory_pool) CMDIdGPDB(oidResult);

	// get output argument types if any
	List *plOutArgTypes = gpdb::GetFuncOutputArgTypes(oidFunc);

	MdidPtrArray *pdrgpmdidArgTypes = NULL;
	if (NULL != plOutArgTypes)
	{
		ListCell *lc = NULL;
		pdrgpmdidArgTypes = GPOS_NEW(memory_pool) MdidPtrArray(memory_pool);

		ForEach (lc, plOutArgTypes)
		{
			OID oidArgType = lfirst_oid(lc);
			GPOS_ASSERT(InvalidOid != oidArgType);
			CMDIdGPDB *pmdidArgType = GPOS_NEW(memory_pool) CMDIdGPDB(oidArgType);
			pdrgpmdidArgTypes->Append(pmdidArgType);
		}

		gpdb::GPDBFree(plOutArgTypes);
	}

	IMDFunction::EFuncStbl stability = IMDFunction::EfsImmutable;
	IMDFunction::EFuncDataAcc access = IMDFunction::EfdaNoSQL;
	BOOL is_strict = true;
	BOOL ReturnsSet = true;
	LookupFuncProps(oidFunc, &stability, &access, &is_strict, &ReturnsSet);

	mdid->AddRef();
	CMDFunctionGPDB *pmdfunc = GPOS_NEW(memory_pool) CMDFunctionGPDB
											(
											memory_pool,
											mdid,
											mdname,
											result_type_mdid,
											pdrgpmdidArgTypes,
											ReturnsSet,
											stability,
											access,
											is_strict
											);

	return pmdfunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveAgg
//
//	@doc:
//		Retrieve an aggregate from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDAggregateGPDB *
CTranslatorRelcacheToDXL::RetrieveAgg
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oidAgg = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidAgg);

	// get agg name
	CHAR *szName = gpdb::GetFuncName(oidAgg);

	if (NULL == szName)
	{

		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CWStringDynamic *pstrAggName = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szName);
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrAggName);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(pstrAggName);

	// get result type
	OID oidResult = gpdb::GetFuncRetType(oidAgg);

	GPOS_ASSERT(InvalidOid != oidResult);

	CMDIdGPDB *result_type_mdid = GPOS_NEW(memory_pool) CMDIdGPDB(oidResult);
	IMDId *intermediate_result_type_mdid = RetrieveAggIntermediateResultType(memory_pool, mdid);

	mdid->AddRef();
	
	BOOL fOrdered = gpdb::IsOrderedAgg(oidAgg);
	
	// GPDB does not support splitting of ordered aggs and aggs without a
	// preliminary function
	BOOL is_splittable = !fOrdered && gpdb::AggHasPrelimFunc(oidAgg);
	
	// cannot use hash agg for ordered aggs or aggs without a prelim func
	// due to the fact that hashAgg may spill
	BOOL is_hash_agg_capable = !fOrdered && gpdb::AggHasPrelimFunc(oidAgg);

	CMDAggregateGPDB *pmdagg = GPOS_NEW(memory_pool) CMDAggregateGPDB
											(
											memory_pool,
											mdid,
											mdname,
											result_type_mdid,
											intermediate_result_type_mdid,
											fOrdered,
											is_splittable,
											is_hash_agg_capable
											);
	return pmdagg;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveTrigger
//
//	@doc:
//		Retrieve a trigger from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDTriggerGPDB *
CTranslatorRelcacheToDXL::RetrieveTrigger
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oidTrigger = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidTrigger);

	// get trigger name
	CHAR *szName = gpdb::GetTriggerName(oidTrigger);

	if (NULL == szName)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	CWStringDynamic *pstrTriggerName = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szName);
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrTriggerName);
	GPOS_DELETE(pstrTriggerName);

	// get relation oid
	OID oidRel = gpdb::GetTriggerRelid(oidTrigger);
	GPOS_ASSERT(InvalidOid != oidRel);
	CMDIdGPDB *pmdidRel = GPOS_NEW(memory_pool) CMDIdGPDB(oidRel);

	// get function oid
	OID oidFunc = gpdb::GetTriggerFuncid(oidTrigger);
	GPOS_ASSERT(InvalidOid != oidFunc);
	CMDIdGPDB *mdid_func = GPOS_NEW(memory_pool) CMDIdGPDB(oidFunc);

	// get type
	INT iType = gpdb::GetTriggerType(oidTrigger);

	// is trigger enabled
	BOOL fEnabled = gpdb::IsTriggerEnabled(oidTrigger);

	mdid->AddRef();
	CMDTriggerGPDB *pmdtrigger = GPOS_NEW(memory_pool) CMDTriggerGPDB
											(
											memory_pool,
											mdid,
											mdname,
											pmdidRel,
											mdid_func,
											iType,
											fEnabled
											);
	return pmdtrigger;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveCheckConstraints
//
//	@doc:
//		Retrieve a check constraint from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDCheckConstraintGPDB *
CTranslatorRelcacheToDXL::RetrieveCheckConstraints
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	OID oidCheckConstraint = CMDIdGPDB::CastMdid(mdid)->OidObjectId();
	GPOS_ASSERT(InvalidOid != oidCheckConstraint);

	// get name of the check constraint
	CHAR *szName = gpdb::GetCheckConstraintName(oidCheckConstraint);
	if (NULL == szName)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}
	CWStringDynamic *pstrCheckConstraintName = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szName);
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrCheckConstraintName);
	GPOS_DELETE(pstrCheckConstraintName);

	// get relation oid associated with the check constraint
	OID oidRel = gpdb::GetCheckConstraintRelid(oidCheckConstraint);
	GPOS_ASSERT(InvalidOid != oidRel);
	CMDIdGPDB *pmdidRel = GPOS_NEW(memory_pool) CMDIdGPDB(oidRel);

	// translate the check constraint expression
	Node *node = gpdb::PnodeCheckConstraint(oidCheckConstraint);
	GPOS_ASSERT(NULL != node);

	CTranslatorScalarToDXL sctranslator
							(
							memory_pool,
							md_accessor,
							NULL, /* pulidgtorCol */
							NULL, /* pulidgtorCTE */
							0, /* query_level */
							true, /* m_fQuery */
							NULL, /* query_level_to_cte_map */
							NULL /* cte_dxlnode_array */
							);

	// generate a mock mapping between var to column information
	CMappingVarColId *var_col_id_mapping = GPOS_NEW(memory_pool) CMappingVarColId(memory_pool);
	ColumnDescrDXLArray *col_descr_dxl_array = GPOS_NEW(memory_pool) ColumnDescrDXLArray(memory_pool);
	const IMDRelation *md_rel = md_accessor->RetrieveRel(pmdidRel);
	const ULONG length = md_rel->ColumnCount();
	for (ULONG ul = 0; ul < length; ul++)
	{
		const IMDColumn *md_col = md_rel->GetMdCol(ul);
		CMDName *md_colname = GPOS_NEW(memory_pool) CMDName(memory_pool, md_col->Mdname().GetMDName());
		CMDIdGPDB *pmdidColType = CMDIdGPDB::CastMdid(md_col->MDIdType());
		pmdidColType->AddRef();

		// create a column descriptor for the column
		CDXLColDescr *dxl_col_descr = GPOS_NEW(memory_pool) CDXLColDescr
										(
										memory_pool,
										md_colname,
										ul + 1 /*col_id*/,
										md_col->AttrNum(),
										pmdidColType,
										md_col->TypeModifier(),
										false /* fColDropped */
										);
		col_descr_dxl_array->Append(dxl_col_descr);
	}
	var_col_id_mapping->LoadColumns(0 /*query_level */, 1 /* rteIndex */, col_descr_dxl_array);

	// translate the check constraint expression
	CDXLNode *pdxlnScalar = sctranslator.CreateScalarOpFromExpr((Expr *) node, var_col_id_mapping);

	// cleanup
	col_descr_dxl_array->Release();
	GPOS_DELETE(var_col_id_mapping);

	mdid->AddRef();

	return GPOS_NEW(memory_pool) CMDCheckConstraintGPDB
						(
						memory_pool,
						mdid,
						mdname,
						pmdidRel,
						pdxlnScalar
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetTypeName
//
//	@doc:
//		Retrieve a type's name from the relcache given its metadata id.
//
//---------------------------------------------------------------------------
CMDName *
CTranslatorRelcacheToDXL::GetTypeName
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oid_type = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(InvalidOid != oid_type);

	CHAR *szTypeName = gpdb::GetTypeName(oid_type);
	GPOS_ASSERT(NULL != szTypeName);

	CWStringDynamic *str_name = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szTypeName);
	CMDName *mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, str_name);

	// cleanup
	GPOS_DELETE(str_name);
	return mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetFuncStability
//
//	@doc:
//		Get function stability property from the GPDB character representation
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncStbl
CTranslatorRelcacheToDXL::GetFuncStability
	(
	CHAR c
	)
{
	CMDFunctionGPDB::EFuncStbl efuncstbl = CMDFunctionGPDB::EfsSentinel;

	switch (c)
	{
		case 's':
			efuncstbl = CMDFunctionGPDB::EfsStable;
			break;
		case 'i':
			efuncstbl = CMDFunctionGPDB::EfsImmutable;
			break;
		case 'v':
			efuncstbl = CMDFunctionGPDB::EfsVolatile;
			break;
		default:
			GPOS_ASSERT(!"Invalid stability property");
	}

	return efuncstbl;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetEFuncDataAccess
//
//	@doc:
//		Get function data access property from the GPDB character representation
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncDataAcc
CTranslatorRelcacheToDXL::GetEFuncDataAccess
	(
	CHAR c
	)
{
	CMDFunctionGPDB::EFuncDataAcc access = CMDFunctionGPDB::EfdaSentinel;

	switch (c)
	{
		case 'n':
			access = CMDFunctionGPDB::EfdaNoSQL;
			break;
		case 'c':
			access = CMDFunctionGPDB::EfdaContainsSQL;
			break;
		case 'r':
			access = CMDFunctionGPDB::EfdaReadsSQLData;
			break;
		case 'm':
			access = CMDFunctionGPDB::EfdaModifiesSQLData;
			break;
		case 's':
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature, GPOS_WSZ_LIT("unknown data access"));
		default:
			GPOS_ASSERT(!"Invalid data access property");
	}

	return access;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveAggIntermediateResultType
//
//	@doc:
//		Retrieve the type id of an aggregate's intermediate results
//
//---------------------------------------------------------------------------
IMDId *
CTranslatorRelcacheToDXL::RetrieveAggIntermediateResultType
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	OID oidAgg = CMDIdGPDB::CastMdid(mdid)->OidObjectId();

	GPOS_ASSERT(InvalidOid != oidAgg);

	OID oidTypeIntermediateResult = gpdb::GetAggIntermediateResultType(oidAgg);
	return GPOS_NEW(memory_pool) CMDIdGPDB(oidTypeIntermediateResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelStats
//
//	@doc:
//		Retrieve relation statistics from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveRelStats
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	CMDIdRelStats *m_rel_stats_mdid = CMDIdRelStats::CastMdid(mdid);
	IMDId *pmdidRel = m_rel_stats_mdid->GetRelMdId();
	OID oidRelation = CMDIdGPDB::CastMdid(pmdidRel)->OidObjectId();

	Relation rel = gpdb::GetRelation(oidRelation);
	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	double rows = 0.0;
	CMDName *mdname = NULL;

	GPOS_TRY
	{
		// get rel name
		CHAR *szRelName = NameStr(rel->rd_rel->relname);
		CWStringDynamic *pstrRelName = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, szRelName);
		mdname = GPOS_NEW(memory_pool) CMDName(memory_pool, pstrRelName);
		// CMDName ctor created a copy of the string
		GPOS_DELETE(pstrRelName);

		BlockNumber pages = 0;
		GpPolicy *pgppolicy = gpdb::GetDistributionPolicy(rel);
		if (!pgppolicy ||pgppolicy->ptype != POLICYTYPE_PARTITIONED)
		{
			gpdb::EstimateRelationSize(rel, NULL, &pages, &rows);
		}
		else
		{
			rows = rel->rd_rel->reltuples;
		}

		m_rel_stats_mdid->AddRef();
		gpdb::CloseRelation(rel);
	}
	GPOS_CATCH_EX(ex)
	{
		gpdb::CloseRelation(rel);
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
	
	BOOL fEmptyStats = false;
	if (rows == 0.0)
	{
		fEmptyStats = true;
	}
		
	CDXLRelStats *pdxlrelstats = GPOS_NEW(memory_pool) CDXLRelStats
												(
												memory_pool,
												m_rel_stats_mdid,
												mdname,
												CDouble(rows),
												fEmptyStats
												);


	return pdxlrelstats;
}

// Retrieve column statistics from relcache
// If all statistics are missing, create dummy statistics
// Also, if the statistics are broken, create dummy statistics
// However, if any statistics are present and not broken,
// create column statistics using these statistics
IMDCacheObject *
CTranslatorRelcacheToDXL::RetrieveColStats
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	IMDId *mdid
	)
{
	CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);
	IMDId *pmdidRel = mdid_col_stats->GetRelMdId();
	ULONG ulPos = mdid_col_stats->Position();
	OID oidRelation = CMDIdGPDB::CastMdid(pmdidRel)->OidObjectId();

	Relation rel = gpdb::GetRelation(oidRelation);
	if (NULL == rel)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	const IMDRelation *md_rel = md_accessor->RetrieveRel(pmdidRel);
	const IMDColumn *md_col = md_rel->GetMdCol(ulPos);
	AttrNumber attrnum = (AttrNumber) md_col->AttrNum();

	// number of rows from pg_class
	CDouble dRows(rel->rd_rel->reltuples);

	// extract column name and type
	CMDName *md_colname = GPOS_NEW(memory_pool) CMDName(memory_pool, md_col->Mdname().GetMDName());
	OID oidAttType = CMDIdGPDB::CastMdid(md_col->MDIdType())->OidObjectId();
	gpdb::CloseRelation(rel);

	DXLBucketPtrArray *stats_bucket_dxl_array = GPOS_NEW(memory_pool) DXLBucketPtrArray(memory_pool);

	if (0 > attrnum)
	{
		mdid_col_stats->AddRef();
		return GenerateStatsForSystemCols
				(
				memory_pool,
				oidRelation,
				mdid_col_stats,
				md_colname,
				oidAttType,
				attrnum,
				stats_bucket_dxl_array,
				dRows
				);
	}

	// extract out histogram and mcv information from pg_statistic
	HeapTuple heaptupleStats = gpdb::GetAttStats(oidRelation, attrnum);

	// if there is no colstats
	if (!HeapTupleIsValid(heaptupleStats))
	{
		stats_bucket_dxl_array->Release();
		mdid_col_stats->AddRef();

		CDouble width = CStatistics::DefaultColumnWidth;

		if (!md_col->IsDropped())
		{
			CMDIdGPDB *pmdidAttType = GPOS_NEW(memory_pool) CMDIdGPDB(oidAttType);
			IMDType *md_type = RetrieveType(memory_pool, pmdidAttType);
			width = CStatisticsUtils::DefaultColumnWidth(md_type);
			md_type->Release();
			pmdidAttType->Release();
		}

		return CDXLColStats::CreateDXLDummyColStats(memory_pool, mdid_col_stats, md_colname, width);
	}

	Form_pg_statistic fpsStats = (Form_pg_statistic) GETSTRUCT(heaptupleStats);

	// null frequency and NDV
	CDouble dNullFrequency(0.0);
	int iNullNDV = 0;
	if (CStatistics::Epsilon < fpsStats->stanullfrac)
	{
		dNullFrequency = fpsStats->stanullfrac;
		iNullNDV = 1;
	}

	// column width
	CDouble width = CDouble(fpsStats->stawidth);

	// calculate total number of distinct values
	CDouble dDistinct(1.0);
	if (fpsStats->stadistinct < 0)
	{
		GPOS_ASSERT(fpsStats->stadistinct > -1.01);
		dDistinct = dRows * CDouble(-fpsStats->stadistinct);
	}
	else
	{
		dDistinct = CDouble(fpsStats->stadistinct);
	}
	dDistinct = dDistinct.Ceil();

	BOOL is_dummy_stats = false;
	// most common values and their frequencies extracted from the pg_statistic
	// tuple for a given column
	AttStatsSlot mcvSlot;

	(void)	gpdb::GetAttrStatsSlot
			(
					&mcvSlot,
					heaptupleStats,
					STATISTIC_KIND_MCV,
					InvalidOid,
					ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS
			);
	if (InvalidOid != mcvSlot.valuetype && mcvSlot.valuetype != oidAttType)
	{
		char msgbuf[NAMEDATALEN * 2 + 100];
		snprintf(msgbuf, sizeof(msgbuf), "Type mismatch between attribute %ls of table %ls having type %d and statistic having type %d, please ANALYZE the table again",
				 md_col->Mdname().GetMDName()->GetBuffer(), md_rel->Mdname().GetMDName()->GetBuffer(), oidAttType, mcvSlot.valuetype);
		GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION,
					NOTICE,
					msgbuf,
					NULL);

		gpdb::FreeAttrStatsSlot(&mcvSlot);
		is_dummy_stats = true;
	}

	else if (mcvSlot.nvalues != mcvSlot.nnumbers)
	{
		char msgbuf[NAMEDATALEN * 2 + 100];
		snprintf(msgbuf, sizeof(msgbuf), "The number of most common values and frequencies do not match on column %ls of table %ls.",
				 md_col->Mdname().GetMDName()->GetBuffer(), md_rel->Mdname().GetMDName()->GetBuffer());
		GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION,
					NOTICE,
					msgbuf,
					NULL);

		// if the number of MCVs(nvalues) and number of MCFs(nnumbers) do not match, we discard the MCVs and MCFs
		gpdb::FreeAttrStatsSlot(&mcvSlot);
		is_dummy_stats = true;
	}
	else
	{
		// fix mcv and null frequencies (sometimes they can add up to more than 1.0)
		NormalizeFrequencies(mcvSlot.numbers, (ULONG) mcvSlot.nvalues, &dNullFrequency);

		// total MCV frequency
		CDouble dMCFSum = 0.0;
		for (int i = 0; i < mcvSlot.nvalues; i++)
		{
			dMCFSum = dMCFSum + CDouble(mcvSlot.numbers[i]);
		}
	}

	// histogram values extracted from the pg_statistic tuple for a given column
	AttStatsSlot histSlot;

	// get histogram datums from pg_statistic entry
	(void) gpdb::GetAttrStatsSlot
			(
					&histSlot,
					heaptupleStats,
					STATISTIC_KIND_HISTOGRAM,
					InvalidOid,
					ATTSTATSSLOT_VALUES
			);

	if (InvalidOid != histSlot.valuetype && histSlot.valuetype != oidAttType)
	{
		char msgbuf[NAMEDATALEN * 2 + 100];
		snprintf(msgbuf, sizeof(msgbuf), "Type mismatch between attribute %ls of table %ls having type %d and statistic having type %d, please ANALYZE the table again",
				 md_col->Mdname().GetMDName()->GetBuffer(), md_rel->Mdname().GetMDName()->GetBuffer(), oidAttType, histSlot.valuetype);
		GpdbEreport(ERRCODE_SUCCESSFUL_COMPLETION,
					NOTICE,
					msgbuf,
					NULL);

		gpdb::FreeAttrStatsSlot(&histSlot);
		is_dummy_stats = true;
	}

	if (is_dummy_stats)
	{
		stats_bucket_dxl_array->Release();
		mdid_col_stats->AddRef();

		CDouble col_width = CStatistics::DefaultColumnWidth;
		gpdb::FreeHeapTuple(heaptupleStats);
		return CDXLColStats::CreateDXLDummyColStats(memory_pool, mdid_col_stats, md_colname, col_width);
	}

	CDouble dNDVBuckets(0.0);
	CDouble dFreqBuckets(0.0);
	CDouble distinct_remaining(0.0);
	CDouble freq_remaining(0.0);

	// We only want to create statistics buckets if the column is NOT a text, varchar, char or bpchar type
	// For the above column types we will use NDVRemain and NullFreq to do cardinality estimation.
	if (CTranslatorUtils::ShouldCreateStatsBucket(oidAttType))
	{
		// transform all the bits and pieces from pg_statistic
		// to a single bucket structure
		DXLBucketPtrArray *stats_bucket_dxl_array_transformed =
		TransformStatsToDXLBucketArray
		(
		 memory_pool,
		 oidAttType,
		 dDistinct,
		 dNullFrequency,
		 mcvSlot.values,
		 mcvSlot.numbers,
		 ULONG(mcvSlot.nvalues),
		 histSlot.values,
		 ULONG(histSlot.nvalues)
		 );

		GPOS_ASSERT(NULL != stats_bucket_dxl_array_transformed);

		const ULONG ulBuckets = stats_bucket_dxl_array_transformed->Size();
		for (ULONG ul = 0; ul < ulBuckets; ul++)
		{
			CDXLBucket *pdxlbucket = (*stats_bucket_dxl_array_transformed)[ul];
			dNDVBuckets = dNDVBuckets + pdxlbucket->GetNumDistinct();
			dFreqBuckets = dFreqBuckets + pdxlbucket->GetFrequency();
		}

		CUtils::AddRefAppend(stats_bucket_dxl_array, stats_bucket_dxl_array_transformed);
		stats_bucket_dxl_array_transformed->Release();

		// there will be remaining tuples if the merged histogram and the NULLS do not cover
		// the total number of distinct values
		if ((1 - CStatistics::Epsilon > dFreqBuckets + dNullFrequency) &&
			(0 < dDistinct - dNDVBuckets - iNullNDV))
		{
			distinct_remaining = std::max(CDouble(0.0), (dDistinct - dNDVBuckets - iNullNDV));
			freq_remaining = std::max(CDouble(0.0), (1 - dFreqBuckets - dNullFrequency));
		}
	}
	else
	{
		// in case of text, varchar, char or bpchar, there are no stats buckets, so the
		// remaining frequency is everything excluding NULLs, and distinct remaining is the
		// stadistinct as available in pg_statistic
		distinct_remaining = dDistinct;
 		freq_remaining = 1 - dNullFrequency;
	}

	// free up allocated datum and float4 arrays
	gpdb::FreeAttrStatsSlot(&mcvSlot);
	gpdb::FreeAttrStatsSlot(&histSlot);

	gpdb::FreeHeapTuple(heaptupleStats);

	// create col stats object
	mdid_col_stats->AddRef();
	CDXLColStats *pdxlcolstats = GPOS_NEW(memory_pool) CDXLColStats
											(
											memory_pool,
											mdid_col_stats,
											md_colname,
											width,
											dNullFrequency,
											distinct_remaining,
											freq_remaining,
											stats_bucket_dxl_array,
											false /* is_col_stats_missing */
											);

	return pdxlcolstats;
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorRelcacheToDXL::GenerateStatsForSystemCols
//
//      @doc:
//              Generate statistics for the system level columns
//
//---------------------------------------------------------------------------
CDXLColStats *
CTranslatorRelcacheToDXL::GenerateStatsForSystemCols
       (
       IMemoryPool *memory_pool,
       OID oidRelation,
       CMDIdColStats *mdid_col_stats,
       CMDName *md_colname,
       OID oidAttType,
       AttrNumber attrnum,
       DXLBucketPtrArray *stats_bucket_dxl_array,
       CDouble dRows
       )
{
       GPOS_ASSERT(NULL != mdid_col_stats);
       GPOS_ASSERT(NULL != md_colname);
       GPOS_ASSERT(InvalidOid != oidAttType);
       GPOS_ASSERT(0 > attrnum);
       GPOS_ASSERT(NULL != stats_bucket_dxl_array);

       CMDIdGPDB *pmdidAttType = GPOS_NEW(memory_pool) CMDIdGPDB(oidAttType);
       IMDType *md_type = RetrieveType(memory_pool, pmdidAttType);
       GPOS_ASSERT(md_type->IsFixedLength());

       BOOL is_col_stats_missing = true;
       CDouble dNullFrequency(0.0);
       CDouble width(md_type->Length());
       CDouble distinct_remaining(0.0);
       CDouble freq_remaining(0.0);

       if (CStatistics::MinRows <= dRows)
	   {
		   switch(attrnum)
			{
				case GpSegmentIdAttributeNumber: // gp_segment_id
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = CDouble(gpdb::GetGPSegmentCount());
						break;
					}
				case TableOidAttributeNumber: // tableoid
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = CDouble(RetrieveNumChildPartitions(oidRelation));
						break;
					}
				case SelfItemPointerAttributeNumber: // ctid
					{
						is_col_stats_missing = false;
						freq_remaining = CDouble(1.0);
						distinct_remaining = dRows;
						break;
					}
				default:
					break;
			}
        }

       // cleanup
       pmdidAttType->Release();
       md_type->Release();

       return GPOS_NEW(memory_pool) CDXLColStats
                       (
                       memory_pool,
                       mdid_col_stats,
                       md_colname,
                       width,
                       dNullFrequency,
                       distinct_remaining,
                       freq_remaining,
                       stats_bucket_dxl_array,
                       is_col_stats_missing
                       );
}


//---------------------------------------------------------------------------
//     @function:
//     CTranslatorRelcacheToDXL::RetrieveNumChildPartitions
//
//  @doc:
//      For non-leaf partition tables return the number of child partitions
//      else return 1
//
//---------------------------------------------------------------------------
ULONG
CTranslatorRelcacheToDXL::RetrieveNumChildPartitions
       (
       OID oidRelation
       )
{
       GPOS_ASSERT(InvalidOid != oidRelation);

       ULONG ulTableCount = gpos::ulong_max;
       if (gpdb::RelPartIsNone(oidRelation))
       {
    	   // not a partitioned table
            ulTableCount = 1;
       }
       else if (gpdb::IsLeafPartition(oidRelation))
       {
           // leaf partition
           ulTableCount = 1;
       }
       else
       {
           ulTableCount = gpdb::CountLeafPartTables(oidRelation);
       }
       GPOS_ASSERT(gpos::ulong_max != ulTableCount);

       return ulTableCount;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetreiveCast
//
//	@doc:
//		Retrieve a cast function from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorRelcacheToDXL::RetreiveCast
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	CMDIdCast *pmdidCast = CMDIdCast::CastMdid(mdid);
	IMDId *pmdidSrc = pmdidCast->MdidSrc();
	IMDId *pmdidDest = pmdidCast->MdidDest();
	IMDCast::EmdCoercepathType coercePathType;

	OID oidSrc = CMDIdGPDB::CastMdid(pmdidSrc)->OidObjectId();
	OID oidDest = CMDIdGPDB::CastMdid(pmdidDest)->OidObjectId();
	CoercionPathType	pathtype;

	OID oidCastFunc = 0;
	BOOL fBinaryCoercible = false;
	
	BOOL fCastExists = gpdb::GetCastFunc(oidSrc, oidDest, &fBinaryCoercible, &oidCastFunc, &pathtype);
	
	if (!fCastExists)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	} 
	
	CHAR *szFuncName = NULL;
	if (InvalidOid != oidCastFunc)
	{
		szFuncName = gpdb::GetFuncName(oidCastFunc);
	}
	else
	{
		// no explicit cast function: use the destination type name as the cast name
		szFuncName = gpdb::GetTypeName(oidDest);
	}
	
	if (NULL == szFuncName)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	mdid->AddRef();
	pmdidSrc->AddRef();
	pmdidDest->AddRef();

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(memory_pool, szFuncName);
	
	switch (pathtype) {
		case COERCION_PATH_ARRAYCOERCE:
		{
			coercePathType = IMDCast::EmdtArrayCoerce;
			return GPOS_NEW(memory_pool) CMDArrayCoerceCastGPDB(memory_pool, mdid, mdname, pmdidSrc, pmdidDest, fBinaryCoercible, GPOS_NEW(memory_pool) CMDIdGPDB(oidCastFunc), IMDCast::EmdtArrayCoerce, default_type_modifier, false, EdxlcfImplicitCast, -1);
		}
			break;
		case COERCION_PATH_FUNC:
			return GPOS_NEW(memory_pool) CMDCastGPDB(memory_pool, mdid, mdname, pmdidSrc, pmdidDest, fBinaryCoercible, GPOS_NEW(memory_pool) CMDIdGPDB(oidCastFunc), IMDCast::EmdtFunc);
			break;
		default:
			break;
	}

	// fall back for none path types
	return GPOS_NEW(memory_pool) CMDCastGPDB(memory_pool, mdid, mdname, pmdidSrc, pmdidDest, fBinaryCoercible, GPOS_NEW(memory_pool) CMDIdGPDB(oidCastFunc));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetreiveScCmp
//
//	@doc:
//		Retrieve a scalar comparison from relcache
//
//---------------------------------------------------------------------------
IMDCacheObject *
CTranslatorRelcacheToDXL::RetreiveScCmp
	(
	IMemoryPool *memory_pool,
	IMDId *mdid
	)
{
	CMDIdScCmp *pmdidScCmp = CMDIdScCmp::CastMdid(mdid);
	IMDId *pmdidLeft = pmdidScCmp->GetLeftMdid();
	IMDId *pmdidRight = pmdidScCmp->GetRightMdid();
	
	IMDType::ECmpType ecmpt = pmdidScCmp->ParseCmpType();

	OID oidLeft = CMDIdGPDB::CastMdid(pmdidLeft)->OidObjectId();
	OID oidRight = CMDIdGPDB::CastMdid(pmdidRight)->OidObjectId();
	CmpType cmpt = (CmpType) GetComparisonType(ecmpt);
	
	OID oidScCmp = gpdb::GetComparisonOperator(oidLeft, oidRight, cmpt);
	
	if (InvalidOid == oidScCmp)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	} 

	CHAR *szName = gpdb::GetOpName(oidScCmp);

	if (NULL == szName)
	{
		GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, mdid->GetBuffer());
	}

	mdid->AddRef();
	pmdidLeft->AddRef();
	pmdidRight->AddRef();

	CMDName *mdname = CDXLUtils::CreateMDNameFromCharArray(memory_pool, szName);

	return GPOS_NEW(memory_pool) CMDScCmpGPDB(memory_pool, mdid, mdname, pmdidLeft, pmdidRight, ecmpt, GPOS_NEW(memory_pool) CMDIdGPDB(oidScCmp));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::TransformStatsToDXLBucketArray
//
//	@doc:
//		transform stats from pg_stats form to optimizer's preferred form
//
//---------------------------------------------------------------------------
DXLBucketPtrArray *
CTranslatorRelcacheToDXL::TransformStatsToDXLBucketArray
	(
	IMemoryPool *memory_pool,
	OID oidAttType,
	CDouble dDistinct,
	CDouble null_freq,
	const Datum *pdrgdatumMCVValues,
	const float4 *pdrgfMCVFrequencies,
	ULONG ulNumMCVValues,
	const Datum *pdrgdatumHistValues,
	ULONG ulNumHistValues
	)
{
	CMDIdGPDB *pmdidAttType = GPOS_NEW(memory_pool) CMDIdGPDB(oidAttType);
	IMDType *md_type = RetrieveType(memory_pool, pmdidAttType);

	// translate MCVs to Orca histogram. Create an empty histogram if there are no MCVs.
	CHistogram *phistGPDBMCV = TransformMcvToOrcaHistogram
							(
							memory_pool,
							md_type,
							pdrgdatumMCVValues,
							pdrgfMCVFrequencies,
							ulNumMCVValues
							);

	GPOS_ASSERT(phistGPDBMCV->IsValid());

	CDouble dMCVFreq = phistGPDBMCV->GetFrequency();
	BOOL fHasMCV = 0 < ulNumMCVValues && CStatistics::Epsilon < dMCVFreq;

	CDouble dHistFreq = 0.0;
	if (1 < ulNumHistValues)
	{
		dHistFreq = CDouble(1.0) - null_freq - dMCVFreq;
	}
	BOOL fHasHist = 1 < ulNumHistValues && CStatistics::Epsilon < dHistFreq;

	CHistogram *histogram = NULL;

	// if histogram has any significant information, then extract it
	if (fHasHist)
	{
		// histogram from gpdb histogram
		histogram = TransformHistToOrcaHistogram
						(
						memory_pool,
						md_type,
						pdrgdatumHistValues,
						ulNumHistValues,
						dDistinct,
						dHistFreq
						);
		if (0 == histogram->Buckets())
		{
			fHasHist = false;
		}
	}

	DXLBucketPtrArray *stats_bucket_dxl_array = NULL;

	if (fHasHist && !fHasMCV)
	{
		// if histogram exists and dominates, use histogram only
		stats_bucket_dxl_array = TransformHistogramToDXLBucketArray(memory_pool, md_type, histogram);
	}
	else if (!fHasHist && fHasMCV)
	{
		// if MCVs exist and dominate, use MCVs only
		stats_bucket_dxl_array = TransformHistogramToDXLBucketArray(memory_pool, md_type, phistGPDBMCV);
	}
	else if (fHasHist && fHasMCV)
	{
		// both histogram and MCVs exist and have significant info, merge MCV and histogram buckets
		CHistogram *phistMerged = CStatisticsUtils::MergeMCVHist(memory_pool, phistGPDBMCV, histogram);
		stats_bucket_dxl_array = TransformHistogramToDXLBucketArray(memory_pool, md_type, phistMerged);
		GPOS_DELETE(phistMerged);
	}
	else
	{
		// no MCVs nor histogram
		GPOS_ASSERT(!fHasHist && !fHasMCV);
		stats_bucket_dxl_array = GPOS_NEW(memory_pool) DXLBucketPtrArray(memory_pool);
	}

	// cleanup
	pmdidAttType->Release();
	md_type->Release();
	GPOS_DELETE(phistGPDBMCV);

	if (NULL != histogram)
	{
		GPOS_DELETE(histogram);
	}

	return stats_bucket_dxl_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::TransformMcvToOrcaHistogram
//
//	@doc:
//		Transform gpdb's mcv info to optimizer histogram
//
//---------------------------------------------------------------------------
CHistogram *
CTranslatorRelcacheToDXL::TransformMcvToOrcaHistogram
	(
	IMemoryPool *memory_pool,
	const IMDType *md_type,
	const Datum *pdrgdatumMCVValues,
	const float4 *pdrgfMCVFrequencies,
	ULONG ulNumMCVValues
	)
{
	DrgPdatum *pdrgpdatum = GPOS_NEW(memory_pool) DrgPdatum(memory_pool);
	DrgPdouble *pdrgpdFreq = GPOS_NEW(memory_pool) DrgPdouble(memory_pool);

	for (ULONG ul = 0; ul < ulNumMCVValues; ul++)
	{
		Datum datumMCV = pdrgdatumMCVValues[ul];
		IDatum *pdatum = CTranslatorScalarToDXL::GetDatum(memory_pool, md_type, false /* is_null */, datumMCV);
		pdrgpdatum->Append(pdatum);
		pdrgpdFreq->Append(GPOS_NEW(memory_pool) CDouble(pdrgfMCVFrequencies[ul]));

		if (!pdatum->StatsAreComparable(pdatum))
		{
			// if less than operation is not supported on this datum, then no point
			// building a histogram. return an empty histogram
			pdrgpdatum->Release();
			pdrgpdFreq->Release();
			return GPOS_NEW(memory_pool) CHistogram(GPOS_NEW(memory_pool) BucketArray(memory_pool));
		}
	}

	CHistogram *phist = CStatisticsUtils::TransformMCVToHist
												(
												memory_pool,
												md_type,
												pdrgpdatum,
												pdrgpdFreq,
												ulNumMCVValues
												);

	pdrgpdatum->Release();
	pdrgpdFreq->Release();
	return phist;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::TransformHistToOrcaHistogram
//
//	@doc:
//		Transform GPDB's hist info to optimizer's histogram
//
//---------------------------------------------------------------------------
CHistogram *
CTranslatorRelcacheToDXL::TransformHistToOrcaHistogram
	(
	IMemoryPool *memory_pool,
	const IMDType *md_type,
	const Datum *pdrgdatumHistValues,
	ULONG ulNumHistValues,
	CDouble dDistinctHist,
	CDouble dFreqHist
	)
{
	GPOS_ASSERT(1 < ulNumHistValues);

	ULONG ulNumBuckets = ulNumHistValues - 1;
	CDouble dDistinctPerBucket = dDistinctHist / CDouble(ulNumBuckets);
	CDouble dFreqPerBucket = dFreqHist / CDouble(ulNumBuckets);

	const ULONG ulBuckets = ulNumHistValues - 1;
	BOOL fLastBucketWasSingleton = false;
	// create buckets
	BucketArray *pdrgppbucket = GPOS_NEW(memory_pool) BucketArray(memory_pool);
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		Datum datumMin = pdrgdatumHistValues[ul];
		IDatum *pdatumMin = CTranslatorScalarToDXL::GetDatum(memory_pool, md_type, false /* is_null */, datumMin);

		Datum datumMax = pdrgdatumHistValues[ul + 1];
		IDatum *pdatumMax = CTranslatorScalarToDXL::GetDatum(memory_pool, md_type, false /* is_null */, datumMax);
		BOOL is_lower_closed, is_upper_closed;

		if (pdatumMin->StatsAreEqual(pdatumMax))
		{
			// Singleton bucket !!!!!!!!!!!!!
			is_lower_closed = true;
			is_upper_closed = true;
			fLastBucketWasSingleton = true;
		}
		else if (fLastBucketWasSingleton)
		{
			// Last bucket was a singleton, so lower must be open now.
			is_lower_closed = false;
			is_upper_closed = false;
			fLastBucketWasSingleton = false;
		}
		else
		{
			// Normal bucket
			// GPDB histograms assumes lower bound to be closed and upper bound to be open
			is_lower_closed = true;
			is_upper_closed = false;
		}

		if (ul == ulBuckets - 1)
		{
			// last bucket upper bound is also closed
			is_upper_closed = true;
		}

		CBucket *pbucket = GPOS_NEW(memory_pool) CBucket
									(
									GPOS_NEW(memory_pool) CPoint(pdatumMin),
									GPOS_NEW(memory_pool) CPoint(pdatumMax),
									is_lower_closed,
									is_upper_closed,
									dFreqPerBucket,
									dDistinctPerBucket
									);
		pdrgppbucket->Append(pbucket);

		if (!pdatumMin->StatsAreComparable(pdatumMin) || !pdatumMin->StatsAreLessThan(pdatumMax))
		{
			// if less than operation is not supported on this datum,
			// or the translated histogram does not conform to GPDB sort order (e.g. text column in Linux platform),
			// then no point building a histogram. return an empty histogram

			// TODO: 03/01/2014 translate histogram into Orca even if sort
			// order is different in GPDB, and use const expression eval to compare
			// datums in Orca (MPP-22780)
			pdrgppbucket->Release();
			return GPOS_NEW(memory_pool) CHistogram(GPOS_NEW(memory_pool) BucketArray(memory_pool));
		}
	}

	CHistogram *phist = GPOS_NEW(memory_pool) CHistogram(pdrgppbucket);
	return phist;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::TransformHistogramToDXLBucketArray
//
//	@doc:
//		Histogram to array of dxl buckets
//
//---------------------------------------------------------------------------
DXLBucketPtrArray *
CTranslatorRelcacheToDXL::TransformHistogramToDXLBucketArray
	(
	IMemoryPool *memory_pool,
	const IMDType *md_type,
	const CHistogram *phist
	)
{
	DXLBucketPtrArray *stats_bucket_dxl_array = GPOS_NEW(memory_pool) DXLBucketPtrArray(memory_pool);
	const BucketArray *pdrgpbucket = phist->ParseDXLToBucketsArray();
	ULONG ulNumBuckets = pdrgpbucket->Size();
	for (ULONG ul = 0; ul < ulNumBuckets; ul++)
	{
		CBucket *pbucket = (*pdrgpbucket)[ul];
		IDatum *pdatumLB = pbucket->GetLowerBound()->GetDatum();
		CDXLDatum *pdxldatumLB = md_type->GetDatumVal(memory_pool, pdatumLB);
		IDatum *pdatumUB = pbucket->GetUpperBound()->GetDatum();
		CDXLDatum *pdxldatumUB = md_type->GetDatumVal(memory_pool, pdatumUB);
		CDXLBucket *pdxlbucket = GPOS_NEW(memory_pool) CDXLBucket
											(
											pdxldatumLB,
											pdxldatumUB,
											pbucket->IsLowerClosed(),
											pbucket->IsUpperClosed(),
											pbucket->GetFrequency(),
											pbucket->GetNumDistinct()
											);
		stats_bucket_dxl_array->Append(pdxlbucket);
	}
	return stats_bucket_dxl_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelStorageType
//
//	@doc:
//		Get relation storage type
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype
CTranslatorRelcacheToDXL::RetrieveRelStorageType
	(
	CHAR cStorageType
	)
{
	IMDRelation::Erelstoragetype rel_storage_type = IMDRelation::ErelstorageSentinel;

	switch (cStorageType)
	{
		case RELSTORAGE_HEAP:
			rel_storage_type = IMDRelation::ErelstorageHeap;
			break;
		case RELSTORAGE_AOCOLS:
			rel_storage_type = IMDRelation::ErelstorageAppendOnlyCols;
			break;
		case RELSTORAGE_AOROWS:
			rel_storage_type = IMDRelation::ErelstorageAppendOnlyRows;
			break;
		case RELSTORAGE_VIRTUAL:
			rel_storage_type = IMDRelation::ErelstorageVirtual;
			break;
		case RELSTORAGE_EXTERNAL:
			rel_storage_type = IMDRelation::ErelstorageExternal;
			break;
		default:
			GPOS_ASSERT(!"Unsupported relation type");
	}

	return rel_storage_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartKeysAndTypes
//
//	@doc:
//		Get partition keys and types for relation or NULL if relation not partitioned.
//		Caller responsible for closing the relation if an exception is raised
//
//---------------------------------------------------------------------------
void
CTranslatorRelcacheToDXL::RetrievePartKeysAndTypes
	(
	IMemoryPool *memory_pool,
	Relation rel,
	OID oid,
	ULongPtrArray **pdrgpulPartKeys,
	CharPtrArray **pdrgpszPartTypes
	)
{
	GPOS_ASSERT(NULL != rel);

	if (!gpdb::RelPartIsRoot(oid))
	{
		// not a partitioned table
		*pdrgpulPartKeys = NULL;
		*pdrgpszPartTypes = NULL;
		return;
	}

	// TODO: Feb 23, 2012; support intermediate levels

	*pdrgpulPartKeys = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	*pdrgpszPartTypes = GPOS_NEW(memory_pool) CharPtrArray(memory_pool);

	List *plPartKeys = NIL;
	List *plPartTypes = NIL;
	gpdb::GetOrderedPartKeysAndKinds(oid, &plPartKeys, &plPartTypes);

	ListCell *plcKey = NULL;
	ListCell *plcType = NULL;
	ForBoth (plcKey, plPartKeys, plcType, plPartTypes)
	{
		List *plPartKey = (List *) lfirst(plcKey);

		if (1 < gpdb::ListLength(plPartKey))
		{
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported, GPOS_WSZ_LIT("Composite part key"));
		}

		INT attno = linitial_int(plPartKey);
		CHAR partType = (CHAR) lfirst_int(plcType);
		GPOS_ASSERT(0 < attno);
		(*pdrgpulPartKeys)->Append(GPOS_NEW(memory_pool) ULONG(attno - 1));
		(*pdrgpszPartTypes)->Append(GPOS_NEW(memory_pool) CHAR(partType));
	}

	gpdb::ListFree(plPartKeys);
	gpdb::ListFree(plPartTypes);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::ConstructAttnoMapping
//
//	@doc:
//		Construct a mapping for GPDB attnos to positions in the columns array
//
//---------------------------------------------------------------------------
ULONG *
CTranslatorRelcacheToDXL::ConstructAttnoMapping
	(
	IMemoryPool *memory_pool,
	MDColumnPtrArray *mdcol_array,
	ULONG ulMaxCols
	)
{
	GPOS_ASSERT(NULL != mdcol_array);
	GPOS_ASSERT(0 < mdcol_array->Size());
	GPOS_ASSERT(ulMaxCols > mdcol_array->Size());

	// build a mapping for attnos->positions
	const ULONG num_of_cols = mdcol_array->Size();
	ULONG *pul = GPOS_NEW_ARRAY(memory_pool, ULONG, ulMaxCols);

	// initialize all positions to gpos::ulong_max
	for (ULONG ul = 0;  ul < ulMaxCols; ul++)
	{
		pul[ul] = gpos::ulong_max;
	}
	
	for (ULONG ul = 0;  ul < num_of_cols; ul++)
	{
		const IMDColumn *md_col = (*mdcol_array)[ul];
		INT attno = md_col->AttrNum();

		ULONG ulIndex = (ULONG) (GPDXL_SYSTEM_COLUMNS + attno);
		pul[ulIndex] = ul;
	}

	return pul;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveRelKeysets
//
//	@doc:
//		Get key sets for relation
//
//---------------------------------------------------------------------------
ULongPtrArray2D *
CTranslatorRelcacheToDXL::RetrieveRelKeysets
	(
	IMemoryPool *memory_pool,
	OID oid,
	BOOL fAddDefaultKeys,
	BOOL fPartitioned,
	ULONG *pulMapping
	)
{
	ULongPtrArray2D *ulong_ptr_array_2D = GPOS_NEW(memory_pool) ULongPtrArray2D(memory_pool);

	List *plKeys = gpdb::GetRelationKeys(oid);

	ListCell *plcKey = NULL;
	ForEach (plcKey, plKeys)
	{
		List *plKey = (List *) lfirst(plcKey);

		ULongPtrArray *pdrgpulKey = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);

		ListCell *plcKeyElem = NULL;
		ForEach (plcKeyElem, plKey)
		{
			INT iKey = lfirst_int(plcKeyElem);
			ULONG ulPos = GetAttributePosition(iKey, pulMapping);
			pdrgpulKey->Append(GPOS_NEW(memory_pool) ULONG(ulPos));
		}
		GPOS_ASSERT(0 < pdrgpulKey->Size());

		ulong_ptr_array_2D->Append(pdrgpulKey);
	}
	
	// add {segid, ctid} as a key
	
	if (fAddDefaultKeys)
	{
		ULongPtrArray *pdrgpulKey = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
		if (fPartitioned)
		{
			// TableOid is part of default key for partitioned tables
			ULONG ulPosTableOid = GetAttributePosition(TableOidAttributeNumber, pulMapping);
			pdrgpulKey->Append(GPOS_NEW(memory_pool) ULONG(ulPosTableOid));
		}
		ULONG ulPosSegid= GetAttributePosition(GpSegmentIdAttributeNumber, pulMapping);
		ULONG ulPosCtid = GetAttributePosition(SelfItemPointerAttributeNumber, pulMapping);
		pdrgpulKey->Append(GPOS_NEW(memory_pool) ULONG(ulPosSegid));
		pdrgpulKey->Append(GPOS_NEW(memory_pool) ULONG(ulPosCtid));
		
		ulong_ptr_array_2D->Append(pdrgpulKey);
	}
	
	return ulong_ptr_array_2D;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::NormalizeFrequencies
//
//	@doc:
//		Sometimes a set of frequencies can add up to more than 1.0.
//		Fix these cases
//
//---------------------------------------------------------------------------
void
CTranslatorRelcacheToDXL::NormalizeFrequencies
	(
	float4 *pdrgf,
	ULONG length,
	CDouble *pdNullFrequency
	)
{
	if (length == 0 && (*pdNullFrequency) < 1.0)
	{
		return;
	}

	CDouble dTotal = *pdNullFrequency;
	for (ULONG ul = 0; ul < length; ul++)
	{
		dTotal = dTotal + CDouble(pdrgf[ul]);
	}

	if (dTotal > CDouble(1.0))
	{
		float4 fDenom = (float4) (dTotal + CStatistics::Epsilon).Get();

		// divide all values by the total
		for (ULONG ul = 0; ul < length; ul++)
		{
			pdrgf[ul] = pdrgf[ul] / fDenom;
		}
		*pdNullFrequency = *pdNullFrequency / fDenom;
	}

#ifdef GPOS_DEBUG
	// recheck
	CDouble dTotalRecheck = *pdNullFrequency;
	for (ULONG ul = 0; ul < length; ul++)
	{
		dTotalRecheck = dTotalRecheck + CDouble(pdrgf[ul]);
	}
	GPOS_ASSERT(dTotalRecheck <= CDouble(1.0));
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::IsIndexSupported
//
//	@doc:
//		Check if index type is supported
//
//---------------------------------------------------------------------------
BOOL
CTranslatorRelcacheToDXL::IsIndexSupported
	(
	Relation relIndex
	)
{
	HeapTupleData *pht = relIndex->rd_indextuple;
	
	// index expressions and index constraints not supported
	return gpdb::HeapAttIsNull(pht, Anum_pg_index_indexprs) &&
		gpdb::HeapAttIsNull(pht, Anum_pg_index_indpred) && 
		(BTREE_AM_OID == relIndex->rd_rel->relam || BITMAP_AM_OID == relIndex->rd_rel->relam);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartConstraintForIndex
//
//	@doc:
//		Retrieve part constraint for index
//
//---------------------------------------------------------------------------
CMDPartConstraintGPDB *
CTranslatorRelcacheToDXL::RetrievePartConstraintForIndex
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	const IMDRelation *md_rel,
	Node *pnodePartCnstr,
	ULongPtrArray *level_with_default_part_array,
	BOOL is_unbounded
	)
{
	ColumnDescrDXLArray *col_descr_dxl_array = GPOS_NEW(memory_pool) ColumnDescrDXLArray(memory_pool);
	const ULONG num_columns = md_rel->ColumnCount();
	
	for (ULONG ul = 0; ul < num_columns; ul++)
	{
		const IMDColumn *md_col = md_rel->GetMdCol(ul);
		CMDName *md_colname = GPOS_NEW(memory_pool) CMDName(memory_pool, md_col->Mdname().GetMDName());
		CMDIdGPDB *pmdidColType = CMDIdGPDB::CastMdid(md_col->MDIdType());
		pmdidColType->AddRef();

		// create a column descriptor for the column
		CDXLColDescr *dxl_col_descr = GPOS_NEW(memory_pool) CDXLColDescr
										(
										memory_pool,
										md_colname,
										ul + 1, // col_id
										md_col->AttrNum(),
										pmdidColType,
										md_col->TypeModifier(),
										false // fColDropped
										);
		col_descr_dxl_array->Append(dxl_col_descr);
	}
	
	CMDPartConstraintGPDB *mdpart_constraint = RetrievePartConstraintFromNode(memory_pool, md_accessor, col_descr_dxl_array, pnodePartCnstr, level_with_default_part_array, is_unbounded);
	
	col_descr_dxl_array->Release();

	return mdpart_constraint;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartConstraintForRel
//
//	@doc:
//		Retrieve part constraint for relation
//
//---------------------------------------------------------------------------
CMDPartConstraintGPDB *
CTranslatorRelcacheToDXL::RetrievePartConstraintForRel
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	OID oidRel,
	MDColumnPtrArray *mdcol_array,
	bool fhasIndex
	)
{
	// get the part constraints
	List *plDefaultLevelsRel = NIL;
	Node *node = gpdb::GetRelationPartContraints(oidRel, &plDefaultLevelsRel);

	// don't retrieve part constraints if there are no indices
	// and no default partitions at any level
	if (!fhasIndex && NIL == plDefaultLevelsRel)
	{
		return NULL;
	}

	List *plPartKeys = gpdb::GetPartitionAttrs(oidRel);
	const ULONG num_of_levels = gpdb::ListLength(plPartKeys);
	gpdb::ListFree(plPartKeys);

	BOOL is_unbounded = true;
	ULongPtrArray *pdrgpulDefaultLevels = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	for (ULONG ul = 0; ul < num_of_levels; ul++)
	{
		if (IsDefaultPartition(plDefaultLevelsRel, ul))
		{
			pdrgpulDefaultLevels->Append(GPOS_NEW(memory_pool) ULONG(ul));
		}
		else
		{
			is_unbounded = false;
		}
	}

	CMDPartConstraintGPDB *mdpart_constraint = NULL;

	if (!fhasIndex)
	{
		// if there are no indices then we don't need to construct the partition constraint
		// expression since ORCA is never going to use it.
		// only send the default partition information.
		pdrgpulDefaultLevels->AddRef();
		mdpart_constraint = GPOS_NEW(memory_pool) CMDPartConstraintGPDB(memory_pool, pdrgpulDefaultLevels, is_unbounded, NULL);
	}
	else
	{
		ColumnDescrDXLArray *col_descr_dxl_array = GPOS_NEW(memory_pool) ColumnDescrDXLArray(memory_pool);
		const ULONG num_columns = mdcol_array->Size();
		for (ULONG ul = 0; ul < num_columns; ul++)
		{
			const IMDColumn *md_col = (*mdcol_array)[ul];
			CMDName *md_colname = GPOS_NEW(memory_pool) CMDName(memory_pool, md_col->Mdname().GetMDName());
			CMDIdGPDB *pmdidColType = CMDIdGPDB::CastMdid(md_col->MDIdType());
			pmdidColType->AddRef();

			// create a column descriptor for the column
			CDXLColDescr *dxl_col_descr = GPOS_NEW(memory_pool) CDXLColDescr
											(
											memory_pool,
											md_colname,
											ul + 1, // col_id
											md_col->AttrNum(),
											pmdidColType,
											md_col->TypeModifier(),
											false // fColDropped
											);
			col_descr_dxl_array->Append(dxl_col_descr);
		}

		mdpart_constraint = RetrievePartConstraintFromNode(memory_pool, md_accessor, col_descr_dxl_array, node, pdrgpulDefaultLevels, is_unbounded);
		col_descr_dxl_array->Release();
	}

	gpdb::ListFree(plDefaultLevelsRel);
	pdrgpulDefaultLevels->Release();

	return mdpart_constraint;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrievePartConstraintFromNode
//
//	@doc:
//		Retrieve part constraint from GPDB node
//
//---------------------------------------------------------------------------
CMDPartConstraintGPDB *
CTranslatorRelcacheToDXL::RetrievePartConstraintFromNode
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	ColumnDescrDXLArray *col_descr_dxl_array,
	Node *pnodeCnstr,
	ULongPtrArray *level_with_default_part_array,
	BOOL is_unbounded
	)
{
	if (NULL == pnodeCnstr)
	{
		return NULL;
	}

	CTranslatorScalarToDXL sctranslator
							(
							memory_pool,
							md_accessor,
							NULL, // pulidgtorCol
							NULL, // pulidgtorCTE
							0, // query_level
							true, // m_fQuery
							NULL, // query_level_to_cte_map
							NULL // cte_dxlnode_array
							);

	// generate a mock mapping between var to column information
	CMappingVarColId *var_col_id_mapping = GPOS_NEW(memory_pool) CMappingVarColId(memory_pool);

	var_col_id_mapping->LoadColumns(0 /*query_level */, 1 /* rteIndex */, col_descr_dxl_array);

	// translate the check constraint expression
	CDXLNode *pdxlnScalar = sctranslator.CreateScalarOpFromExpr((Expr *) pnodeCnstr, var_col_id_mapping);

	// cleanup
	GPOS_DELETE(var_col_id_mapping);

	level_with_default_part_array->AddRef();
	return GPOS_NEW(memory_pool) CMDPartConstraintGPDB(memory_pool, level_with_default_part_array, is_unbounded, pdxlnScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RelHasSystemColumns
//
//	@doc:
//		Does given relation type have system columns.
//		Currently only regular relations, sequences, toast values relations and
//		AO segment relations have system columns
//
//---------------------------------------------------------------------------
BOOL
CTranslatorRelcacheToDXL::RelHasSystemColumns
	(
	char cRelKind
	)
{
	return RELKIND_RELATION == cRelKind || 
			RELKIND_SEQUENCE == cRelKind || 
			RELKIND_AOSEGMENTS == cRelKind ||
			RELKIND_TOASTVALUE == cRelKind;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::ParseCmpType
//
//	@doc:
//		Translate GPDB comparison types into optimizer comparison types
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CTranslatorRelcacheToDXL::ParseCmpType
	(
	ULONG ulCmpt
	)
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgulCmpTypeMappings); ul++)
	{
		const ULONG *pul = rgulCmpTypeMappings[ul];
		if (pul[1] == ulCmpt)
		{
			return (IMDType::ECmpType) pul[0];
		}
	}
	
	return IMDType::EcmptOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::GetComparisonType
//
//	@doc:
//		Translate optimizer comparison types into GPDB comparison types
//
//---------------------------------------------------------------------------
ULONG 
CTranslatorRelcacheToDXL::GetComparisonType
	(
	IMDType::ECmpType ecmpt
	)
{
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgulCmpTypeMappings); ul++)
	{
		const ULONG *pul = rgulCmpTypeMappings[ul];
		if (pul[0] == ecmpt)
		{
			return (ULONG) pul[1];
		}
	}
	
	return CmptOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveIndexOpFamilies
//
//	@doc:
//		Retrieve the opfamilies for the keys of the given index
//
//---------------------------------------------------------------------------
MdidPtrArray * 
CTranslatorRelcacheToDXL::RetrieveIndexOpFamilies
	(
	IMemoryPool *memory_pool,
	IMDId *mdid_index
	)
{
	List *plOpFamilies = gpdb::GetIndexOpFamilies(CMDIdGPDB::CastMdid(mdid_index)->OidObjectId());
	MdidPtrArray *input_col_mdids = GPOS_NEW(memory_pool) MdidPtrArray(memory_pool);
	
	ListCell *lc = NULL;
	
	ForEach(lc, plOpFamilies)
	{
		OID op_family_oid = lfirst_oid(lc);
		input_col_mdids->Append(GPOS_NEW(memory_pool) CMDIdGPDB(op_family_oid));
	}
	
	return input_col_mdids;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorRelcacheToDXL::RetrieveScOpOpFamilies
//
//	@doc:
//		Retrieve the families for the keys of the given scalar operator
//
//---------------------------------------------------------------------------
MdidPtrArray * 
CTranslatorRelcacheToDXL::RetrieveScOpOpFamilies
	(
	IMemoryPool *memory_pool,
	IMDId *pmdidScOp
	)
{
	List *plOpFamilies = gpdb::GetOpFamiliesForScOp(CMDIdGPDB::CastMdid(pmdidScOp)->OidObjectId());
	MdidPtrArray *input_col_mdids = GPOS_NEW(memory_pool) MdidPtrArray(memory_pool);
	
	ListCell *lc = NULL;
	
	ForEach(lc, plOpFamilies)
	{
		OID op_family_oid = lfirst_oid(lc);
		input_col_mdids->Append(GPOS_NEW(memory_pool) CMDIdGPDB(op_family_oid));
	}
	
	return input_col_mdids;
}

// EOF

