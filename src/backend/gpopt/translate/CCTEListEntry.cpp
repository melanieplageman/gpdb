//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCTEListEntry.cpp
//
//	@doc:
//		Implementation of the class representing the list of common table
//		expression defined at a query level
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "gpopt/translate/CCTEListEntry.h"

#include "nodes/parsenodes.h"

#include "gpos/base.h"
#include "gpopt/gpdbwrappers.h"
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CCTEListEntry::CCTEListEntry
//
//	@doc:
//		Ctor: single CTE
//
//---------------------------------------------------------------------------
CCTEListEntry::CCTEListEntry
	(
	IMemoryPool *memory_pool,
	ULONG query_level,
	CommonTableExpr *pcte,
	CDXLNode *pdxlnCTEProducer
	)
	:
	m_ulQueryLevel(query_level),
	m_phmszcteinfo(NULL)
{
	GPOS_ASSERT(NULL != pcte && NULL != pdxlnCTEProducer);
	
	m_phmszcteinfo = GPOS_NEW(memory_pool) HMSzCTEInfo(memory_pool);
	Query *pqueryCTE = (Query*) pcte->ctequery;
		
#ifdef GPOS_DEBUG
		BOOL result =
#endif
	m_phmszcteinfo->Insert(pcte->ctename, GPOS_NEW(memory_pool) SCTEProducerInfo(pdxlnCTEProducer, pqueryCTE->targetList));
		
	GPOS_ASSERT(result);
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEListEntry::CCTEListEntry
//
//	@doc:
//		Ctor: multiple CTEs
//
//---------------------------------------------------------------------------
CCTEListEntry::CCTEListEntry
	(
	IMemoryPool *memory_pool,
	ULONG query_level,
	List *plCTE, 
	DXLNodeArray *pdrgpdxln
	)
	:
	m_ulQueryLevel(query_level),
	m_phmszcteinfo(NULL)
{
	GPOS_ASSERT(NULL != pdrgpdxln);
	GPOS_ASSERT(pdrgpdxln->Size() == gpdb::ListLength(plCTE));
	
	m_phmszcteinfo = GPOS_NEW(memory_pool) HMSzCTEInfo(memory_pool);
	const ULONG ulCTEs = pdrgpdxln->Size();
	
	for (ULONG ul = 0; ul < ulCTEs; ul++)
	{
		CDXLNode *pdxlnCTEProducer = (*pdrgpdxln)[ul];
		CommonTableExpr *pcte = (CommonTableExpr*) gpdb::PvListNth(plCTE, ul);

		Query *pqueryCTE = (Query*) pcte->ctequery;
		
#ifdef GPOS_DEBUG
		BOOL result =
#endif
		m_phmszcteinfo->Insert(pcte->ctename, GPOS_NEW(memory_pool) SCTEProducerInfo(pdxlnCTEProducer, pqueryCTE->targetList));
		
		GPOS_ASSERT(result);
		GPOS_ASSERT(NULL != m_phmszcteinfo->Find(pcte->ctename));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEListEntry::PdxlnCTEProducer
//
//	@doc:
//		Return the query of the CTE referenced in the range table entry
//
//---------------------------------------------------------------------------
const CDXLNode *
CCTEListEntry::PdxlnCTEProducer
	(
	const CHAR *szCTE
	)
	const
{
	SCTEProducerInfo *pcteinfo = m_phmszcteinfo->Find(szCTE);
	if (NULL == pcteinfo)
	{
		return NULL; 
	}
	
	return pcteinfo->m_pdxlnCTEProducer;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEListEntry::PdxlnCTEProducer
//
//	@doc:
//		Return the query of the CTE referenced in the range table entry
//
//---------------------------------------------------------------------------
List *
CCTEListEntry::PlCTEProducerTL
	(
	const CHAR *szCTE
	)
	const
{
	SCTEProducerInfo *pcteinfo = m_phmszcteinfo->Find(szCTE);
	if (NULL == pcteinfo)
	{
		return NULL; 
	}
	
	return pcteinfo->m_plTargetList;
}

//---------------------------------------------------------------------------
//	@function:
//		CCTEListEntry::AddCTEProducer
//
//	@doc:
//		Add a new CTE producer to this query level
//
//---------------------------------------------------------------------------
void
CCTEListEntry::AddCTEProducer
	(
	IMemoryPool *memory_pool,
	CommonTableExpr *pcte,
	const CDXLNode *pdxlnCTEProducer
	)
{
	GPOS_ASSERT(NULL == m_phmszcteinfo->Find(pcte->ctename) && "CTE entry already exists");
	Query *pqueryCTE = (Query*) pcte->ctequery;
	
#ifdef GPOS_DEBUG
	BOOL result =
#endif
	m_phmszcteinfo->Insert(pcte->ctename, GPOS_NEW(memory_pool) SCTEProducerInfo(pdxlnCTEProducer, pqueryCTE->targetList));
	
	GPOS_ASSERT(result);
}

// EOF
