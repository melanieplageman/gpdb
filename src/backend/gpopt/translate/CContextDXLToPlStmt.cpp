//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CContextDXLToPlStmt.cpp
//
//	@doc:
//		Implementation of the functions that provide
//		access to CIdGenerators (needed to number initplans, motion
//		nodes as well as params), list of RangeTableEntires and Subplans
//		generated so far during DXL-->PlStmt translation.
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#include "gpopt/translate/CContextDXLToPlStmt.h"
#include "gpopt/gpdbwrappers.h"
#include "gpos/base.h"
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::CContextDXLToPlStmt
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CContextDXLToPlStmt::CContextDXLToPlStmt
	(
	IMemoryPool *memory_pool,
	CIdGenerator *pidgtorPlan,
	CIdGenerator *pidgtorMotion,
	CIdGenerator *pidgtorParam,
	List **plRTable,
	List **plSubPlan
	)
	:
	m_memory_pool(memory_pool),
	m_pidgtorPlan(pidgtorPlan),
	m_pidgtorMotion(pidgtorMotion),
	m_pidgtorParam(pidgtorParam),
	m_pplRTable(plRTable),
	m_plPartitionTables(NULL),
	m_pdrgpulNumSelectors(NULL),
	m_pplSubPlan(plSubPlan),
	m_ulResultRelation(0),
	m_pintocl(NULL),
	m_pdistrpolicy(NULL)
{
	m_phmulcteconsumerinfo = GPOS_NEW(m_memory_pool) HMUlCTEConsumerInfo(m_memory_pool);
	m_pdrgpulNumSelectors = GPOS_NEW(m_memory_pool) ULongPtrArray(m_memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::~CContextDXLToPlStmt
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CContextDXLToPlStmt::~CContextDXLToPlStmt()
{
	m_phmulcteconsumerinfo->Release();
	m_pdrgpulNumSelectors->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::UlNextPlanId
//
//	@doc:
//		Get the next plan id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::UlNextPlanId()
{
	return m_pidgtorPlan->next_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::UlCurrentMotionId
//
//	@doc:
//		Get the current motion id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::UlCurrentMotionId()
{
	return m_pidgtorMotion->current_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::UlNextMotionId
//
//	@doc:
//		Get the next motion id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::UlNextMotionId()
{
	return m_pidgtorMotion->next_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::UlNextParamId
//
//	@doc:
//		Get the next plan id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::UlNextParamId()
{
	return m_pidgtorParam->next_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::UlCurrentParamId
//
//	@doc:
//		Get the current param id
//
//---------------------------------------------------------------------------
ULONG
CContextDXLToPlStmt::UlCurrentParamId()
{
	return m_pidgtorParam->current_id();
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddCTEConsumerInfo
//
//	@doc:
//		Add information about the newly found CTE entry
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddCTEConsumerInfo
	(
	ULONG ulCteId,
	ShareInputScan *pshscan
	)
{
	GPOS_ASSERT(NULL != pshscan);

	SCTEConsumerInfo *pcteinfo = m_phmulcteconsumerinfo->Find(&ulCteId);
	if (NULL != pcteinfo)
	{
		pcteinfo->AddCTEPlan(pshscan);
		return;
	}

	List *plPlanCTE = ListMake1(pshscan);

	ULONG *pulKey = GPOS_NEW(m_memory_pool) ULONG(ulCteId);
#ifdef GPOS_DEBUG
	BOOL result =
#endif
			m_phmulcteconsumerinfo->Insert(pulKey, GPOS_NEW(m_memory_pool) SCTEConsumerInfo(plPlanCTE));

	GPOS_ASSERT(result);
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::PplanCTEProducer
//
//	@doc:
//		Return the list of GPDB plan nodes representing the CTE consumers
//		with the given CTE identifier
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::PshscanCTEConsumer
	(
	ULONG ulCteId
	)
	const
{
	SCTEConsumerInfo *pcteinfo = m_phmulcteconsumerinfo->Find(&ulCteId);
	if (NULL != pcteinfo)
	{
		return pcteinfo->m_plSis;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::PlPrte
//
//	@doc:
//		Return the list of RangeTableEntries
//
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::PlPrte()
{
	return (*(m_pplRTable));
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::PlPplanSubplan
//
//	@doc:
//		Return the list of subplans generated so far
//
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::PlPplanSubplan()
{
	return (*(m_pplSubPlan));
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddRTE
//
//	@doc:
//		Add a RangeTableEntries
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddRTE
	(
	RangeTblEntry *prte,
	BOOL fResultRelation
	)
{
	(* (m_pplRTable)) = gpdb::PlAppendElement((*(m_pplRTable)), prte);

	prte->inFromCl = true;

	if (fResultRelation)
	{
		GPOS_ASSERT(0 == m_ulResultRelation && "Only one result relation supported");
		prte->inFromCl = false;
		m_ulResultRelation = gpdb::ListLength(*(m_pplRTable));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddPartitionedTable
//
//	@doc:
//		Add a partitioned table oid
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddPartitionedTable
	(
	OID oid
	)
{
	if (!gpdb::FMemberOid(m_plPartitionTables, oid))
	{
		m_plPartitionTables = gpdb::PlAppendOid(m_plPartitionTables, oid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::IncrementPartitionSelectors
//
//	@doc:
//		Increment the number of partition selectors for the given scan id
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::IncrementPartitionSelectors
	(
	ULONG scan_id
	)
{
	// add extra elements to the array if necessary
	const ULONG ulLen = m_pdrgpulNumSelectors->Size();
	for (ULONG ul = ulLen; ul <= scan_id; ul++)
	{
		ULONG *pul = GPOS_NEW(m_memory_pool) ULONG(0);
		m_pdrgpulNumSelectors->Append(pul);
	}

	ULONG *pul = (*m_pdrgpulNumSelectors)[scan_id];
	(*pul) ++;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::PlNumPartitionSelectors
//
//	@doc:
//		Return list containing number of partition selectors for every scan id
//
//---------------------------------------------------------------------------
List *
CContextDXLToPlStmt::PlNumPartitionSelectors() const
{
	List *pl = NIL;
	const ULONG ulLen = m_pdrgpulNumSelectors->Size();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG *pul = (*m_pdrgpulNumSelectors)[ul];
		pl = gpdb::PlAppendInt(pl, *pul);
	}

	return pl;
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddSubplan
//
//	@doc:
//		Add a subplan
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddSubplan(Plan *pplan)
{
	(* (m_pplSubPlan)) = gpdb::PlAppendElement((*(m_pplSubPlan)), pplan);
}

//---------------------------------------------------------------------------
//	@function:
//		CContextDXLToPlStmt::AddCtasInfo
//
//	@doc:
//		Add CTAS info
//
//---------------------------------------------------------------------------
void
CContextDXLToPlStmt::AddCtasInfo
	(
	IntoClause *pintocl,
	GpPolicy *pdistrpolicy
	)
{
	GPOS_ASSERT(NULL != pintocl);
	GPOS_ASSERT(NULL != pdistrpolicy);
	
	m_pintocl = pintocl;
	m_pdistrpolicy = pdistrpolicy;
}

// EOF
