//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CConstExprEvaluatorProxy.h
//
//	@doc:
//		Evaluator for constant expressions passed as DXL
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CConstExprEvaluator_H
#define GPDXL_CConstExprEvaluator_H

#include "gpos/base.h"

#include "gpopt/eval/IConstDXLNodeEvaluator.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CMappingColIdVar.h"
#include "gpopt/translate/CTranslatorDXLToScalar.h"

namespace gpdxl
{
	class CDXLNode;

	//---------------------------------------------------------------------------
	//	@class:
	//		CConstExprEvaluatorProxy
	//
	//	@doc:
	//		Wrapper over GPDB's expression evaluator that takes a constant expression,
	//		given as DXL, tries to evaluate it and returns the result as DXL.
	//
	//		The metadata cache should have been initialized by the caller before
	//		creating an instance of this class and should not be released before
	//		the destructor of this class.
	//
	//---------------------------------------------------------------------------
	class CConstExprEvaluatorProxy : public gpopt::IConstDXLNodeEvaluator
	{
		private:
			//---------------------------------------------------------------------------
			//	@class:
			//		CEmptyMappingColIdVar
			//
			//	@doc:
			//		Dummy class to implement an empty variable mapping. Variable lookups
			//		raise exceptions.
			//
			//---------------------------------------------------------------------------
			class CEmptyMappingColIdVar : public CMappingColIdVar
			{
				public:
					explicit
					CEmptyMappingColIdVar
						(
						IMemoryPool *memory_pool
						)
						:
						CMappingColIdVar(memory_pool)
					{
					}

					virtual
					~CEmptyMappingColIdVar()
					{
					}

					virtual
					Var *PvarFromDXLNodeScId(const CDXLScalarIdent *scalar_ident);

			};

			// memory pool, not owned
			IMemoryPool *m_memory_pool;

			// empty mapping needed for the translator
			CEmptyMappingColIdVar m_emptymapcidvar;

			// pointer to metadata cache accessor
			CMDAccessor *m_mda;

			// translator for the DXL input -> GPDB Expr
			CTranslatorDXLToScalar m_dxl2scalar_translator;

		public:
			// ctor
			CConstExprEvaluatorProxy
				(
				IMemoryPool *memory_pool,
				CMDAccessor *md_accessor
				)
				:
				m_memory_pool(memory_pool),
				m_emptymapcidvar(m_memory_pool),
				m_mda(md_accessor),
				m_dxl2scalar_translator(m_memory_pool, m_mda, 0)
			{
			}

			// dtor
			virtual
			~CConstExprEvaluatorProxy()
			{
			}

			// evaluate given constant expressionand return the DXL representation of the result.
			// if the expression has variables, an error is thrown.
			// caller keeps ownership of 'pdxlnExpr' and takes ownership of the returned pointer
			virtual
			CDXLNode *PdxlnEvaluateExpr(const CDXLNode *expr);

			// returns true iff the evaluator can evaluate constant expressions without subqueries
			virtual
			BOOL FCanEvalExpressions()
			{
				return true;
			}
	};
}

#endif // !GPDXL_CConstExprEvaluator_H

// EOF
