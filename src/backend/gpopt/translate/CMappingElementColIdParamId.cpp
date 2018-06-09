//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMappingElementColIdParamId.cpp
//
//	@doc:
//		Implementation of the functions for the mapping element between ColId
//		and ParamId during DXL->PlStmt translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "postgres.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"

#include "gpopt/translate/CMappingElementColIdParamId.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMappingElementColIdParamId::CMappingElementColIdParamId
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMappingElementColIdParamId::CMappingElementColIdParamId
	(
	ULONG col_id,
	ULONG ulParamId,
	IMDId *pmdid,
	INT type_modifier
	)
	:
	m_colid(col_id),
	m_ulParamId(ulParamId),
	m_mdid(pmdid),
	m_type_modifier(type_modifier)
{
}

// EOF
