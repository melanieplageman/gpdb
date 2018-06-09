//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CGPDBAttInfo.h
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttInfo_H
#define GPDXL_CGPDBAttInfo_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"
#include "naucrates/dxl/gpdb_types.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CGPDBAttInfo
	//
	//	@doc:
	//		Class to uniquely identify a column in GPDB
	//
	//---------------------------------------------------------------------------
	class CGPDBAttInfo: public CRefCount
	{
		private:

			// query level number
			ULONG m_ulQueryLevel;

			// varno in the rtable
			ULONG m_ulVarNo;

			// attno
			INT	m_iAttNo;

			// copy c'tor
			CGPDBAttInfo(const CGPDBAttInfo&);

		public:
			// ctor
			CGPDBAttInfo(ULONG query_level, ULONG var_no, INT attrnum)
				: m_ulQueryLevel(query_level), m_ulVarNo(var_no), m_iAttNo(attrnum)
			{}

			// d'tor
			virtual
			~CGPDBAttInfo() {}

			// accessor
			ULONG QueryLevel() const
			{
				return m_ulQueryLevel;
			}

			// accessor
			ULONG UlVarNo() const
			{
				return m_ulVarNo;
			}

			// accessor
			INT IAttNo() const
			{
				return m_iAttNo;
			}

			// equality check
			BOOL Equals(const CGPDBAttInfo& gpdbattinfo) const
			{
				return m_ulQueryLevel == gpdbattinfo.m_ulQueryLevel
						&& m_ulVarNo == gpdbattinfo.m_ulVarNo
						&& m_iAttNo == gpdbattinfo.m_iAttNo;
			}

			// hash value
			ULONG HashValue() const
			{
				return gpos::CombineHashes(
						gpos::HashValue(&m_ulQueryLevel),
						gpos::CombineHashes(gpos::HashValue(&m_ulVarNo),
								gpos::HashValue(&m_iAttNo)));
			}
	};

	// hash function
	inline ULONG UlHashGPDBAttInfo
		(
		const CGPDBAttInfo *gpdb_att_info
		)
	{
		GPOS_ASSERT(NULL != gpdb_att_info);
		return gpdb_att_info->HashValue();
	}

	// equality function
	inline BOOL FEqualGPDBAttInfo
		(
		const CGPDBAttInfo *pgpdbattinfoA,
		const CGPDBAttInfo *pgpdbattinfoB
		)
	{
		GPOS_ASSERT(NULL != pgpdbattinfoA && NULL != pgpdbattinfoB);
		return pgpdbattinfoA->Equals(*pgpdbattinfoB);
	}

}

#endif // !GPDXL_CGPDBAttInfo_H

// EOF
