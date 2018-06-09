//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTranslateContext.cpp
//
//	@doc:
//		Implementation of the methods for accessing translation context
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/translate/CDXLTranslateContext.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::CDXLTranslateContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLTranslateContext::CDXLTranslateContext
	(
	IMemoryPool *memory_pool,
	BOOL fChildAggNode
	)
	:
	m_memory_pool(memory_pool),
	m_fChildAggNode(fChildAggNode)
{
	// initialize hash table
	m_phmulte = GPOS_NEW(m_memory_pool) HMUlTe(m_memory_pool);
	m_phmcolparam = GPOS_NEW(m_memory_pool) HMColParam(m_memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::CDXLTranslateContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLTranslateContext::CDXLTranslateContext
	(
	IMemoryPool *memory_pool,
	BOOL fChildAggNode,
	HMColParam *phmOriginal
	)
	:
	m_memory_pool(memory_pool),
	m_fChildAggNode(fChildAggNode)
{
	m_phmulte = GPOS_NEW(m_memory_pool) HMUlTe(m_memory_pool);
	m_phmcolparam = GPOS_NEW(m_memory_pool) HMColParam(m_memory_pool);
	CopyParamHashmap(phmOriginal);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::~CDXLTranslateContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLTranslateContext::~CDXLTranslateContext()
{
	m_phmulte->Release();
	m_phmcolparam->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::FParentAggNode
//
//	@doc:
//		Is this translation context created by a parent Agg node
//
//---------------------------------------------------------------------------
BOOL
CDXLTranslateContext::FParentAggNode() const
{
	return m_fChildAggNode;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::CopyParamHashmap
//
//	@doc:
//		copy the params hashmap
//
//---------------------------------------------------------------------------
void
CDXLTranslateContext::CopyParamHashmap
	(
	HMColParam *phmOriginal
	)
{
	// iterate over full map
	HMColParamIter hashmapiter(phmOriginal);
	while (hashmapiter.Advance())
	{
		CMappingElementColIdParamId *pmecolidparamid = const_cast<CMappingElementColIdParamId *>(hashmapiter.Value());

		const ULONG col_id = pmecolidparamid->GetColId();
		ULONG *pulKey = GPOS_NEW(m_memory_pool) ULONG(col_id);
		pmecolidparamid->AddRef();
		m_phmcolparam->Insert(pulKey, pmecolidparamid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::Pte
//
//	@doc:
//		Lookup target entry associated with a given col id
//
//---------------------------------------------------------------------------
const TargetEntry *
CDXLTranslateContext::Pte
	(
	ULONG col_id
	)
	const
{
	return m_phmulte->Find(&col_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::Pmecolidparamid
//
//	@doc:
//		Lookup col->param mapping associated with a given col id
//
//---------------------------------------------------------------------------
const CMappingElementColIdParamId *
CDXLTranslateContext::Pmecolidparamid
	(
	ULONG col_id
	)
	const
{
	return m_phmcolparam->Find(&col_id);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::InsertMapping
//
//	@doc:
//		Insert a (col id, target entry) mapping
//
//---------------------------------------------------------------------------
void
CDXLTranslateContext::InsertMapping
	(
	ULONG col_id,
	TargetEntry *target_entry
	)
{
	// copy key
	ULONG *pulKey = GPOS_NEW(m_memory_pool) ULONG(col_id);

	// insert colid->target entry mapping in the hash map
	BOOL result = m_phmulte->Insert(pulKey, target_entry);

	if (!result)
	{
		GPOS_DELETE(pulKey);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTranslateContext::FInsertParamMapping
//
//	@doc:
//		Insert a (col id, param id) mapping
//
//---------------------------------------------------------------------------
BOOL
CDXLTranslateContext::FInsertParamMapping
	(
	ULONG col_id,
	CMappingElementColIdParamId *pmecolidparamid
	)
{
	// copy key
	ULONG *pulKey = GPOS_NEW(m_memory_pool) ULONG(col_id);

	// insert colid->target entry mapping in the hash map
	return m_phmcolparam->Insert(pulKey, pmecolidparamid);
}

// EOF
