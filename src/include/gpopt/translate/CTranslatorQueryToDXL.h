//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CTranslatorQueryToDXL.h
//
//	@doc:
//		Class providing methods for translating a GPDB Query object into a
//		DXL Tree
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorQueryToDXL_H
#define GPDXL_CTranslatorQueryToDXL_H

#define GPDXL_CTE_ID_START 1
#define GPDXL_COL_ID_START 1

#include "gpopt/translate/CMappingVarColId.h"
#include "gpopt/translate/CTranslatorScalarToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"

// fwd declarations
namespace gpopt
{
	class CMDAccessor;
}

struct Query;
struct RangeTblEntry;
struct Const;
struct List;
struct CommonTableExpr;

namespace gpdxl
{
	using namespace gpos;
	using namespace gpopt;

	typedef CHashMap<ULONG, BOOL, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
			CleanupDelete<ULONG>, CleanupDelete<BOOL> > UlongBoolHashMap;

	typedef CHashMapIter<INT, ULONG, gpos::HashValue<INT>, gpos::Equals<INT>,
			CleanupDelete<INT>, CleanupDelete<ULONG> > IntUlongHashmapIter;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorQueryToDXL
	//
	//	@doc:
	//		Class providing methods for translating a GPDB Query object into a
	//      DXL Tree.
	//
	//---------------------------------------------------------------------------
	class CTranslatorQueryToDXL
	{
		// shorthand for functions for translating DXL nodes to GPDB expressions
		typedef CDXLNode * (CTranslatorQueryToDXL::*DXLNodeToLogicalFunc)(const RangeTblEntry *prte, ULONG ulRTIndex, ULONG ulCurrQueryLevel);

		// mapping RTEKind to WCHARs
		struct SRTENameElem
		{
			RTEKind m_rtekind;
			const WCHAR *m_wsz;
		};

		// pair of RTEKind and its translators
		struct SRTETranslator
		{
			RTEKind m_rtekind;
			DXLNodeToLogicalFunc pf;
		};

		// mapping CmdType to WCHARs
		struct SCmdNameElem
		{
			CmdType m_cmdtype;
			const WCHAR *m_wsz;
		};

		// pair of unsupported node tag and feature name
		struct SUnsupportedFeature
		{
			NodeTag ent;
			const WCHAR *m_wsz;
		};
		
		private:
			// memory pool
			IMemoryPool *m_memory_pool;

			// source system id
			CSystemId m_sysid;

			// meta data accessor
			CMDAccessor *m_md_accessor;

			// counter for generating unique column ids
			CIdGenerator *m_pidgtorCol;

			// counter for generating unique CTE ids
			CIdGenerator *m_pidgtorCTE;

			// scalar translator used to convert scalar operation into DXL.
			CTranslatorScalarToDXL *m_psctranslator;

			// holds the var to col id information mapping
			CMappingVarColId *m_pmapvarcolid;

			// query being translated
			Query *m_pquery;

			// absolute level of query being translated
			ULONG m_query_level;

			// does the query have distributed tables
			BOOL m_fHasDistributedTables;

			// top query is a DML
			BOOL m_fTopDMLQuery;

			// this is a CTAS query
			BOOL m_fCTASQuery;

			// hash map that maintains the list of CTEs defined at a particular query level
			HMUlCTEListEntry *m_phmulCTEEntries;

			// query output columns
			DXLNodeArray *m_pdrgpdxlnQueryOutput;
			
			// list of CTE producers
			DXLNodeArray *m_pdrgpdxlnCTE;
			
			// CTE producer IDs defined at the current query level
			UlongBoolHashMap *m_phmulfCTEProducers;

			//ctor
			// private constructor, called from the public factory function QueryToDXLInstance
			CTranslatorQueryToDXL
				(
				IMemoryPool *memory_pool,
				CMDAccessor *md_accessor,
				CIdGenerator *pidgtorColId,
				CIdGenerator *pidgtorCTE,
				CMappingVarColId *var_col_id_mapping,
				Query *query,
				ULONG query_level,
				BOOL fTopDMLQuery,
				HMUlCTEListEntry *phmulCTEEntries  // hash map between query level -> list of CTEs defined at that level
				);

			// private copy ctor
			CTranslatorQueryToDXL(const CTranslatorQueryToDXL&);

			// check for unsupported node types, throws an exception if an unsupported
			// node is found
			void CheckUnsupportedNodeTypes(Query *query);

			// check for SIRV functions in the targetlist without a FROM clause and
			// throw an exception when found
			void CheckSirvFuncsWithoutFromClause(Query *query);

			// check for SIRV functions in the tree rooted at the given node
			BOOL HasSirvFunctions (Node *node) const;

			// translate FromExpr (in the GPDB query) into a CDXLLogicalJoin or CDXLLogicalGet
			CDXLNode *TranslateFromExprToDXL(FromExpr *pfromexpr);

			// translate set operations
			CDXLNode *TranslateSetOpToDXL(Node *pnodeSetOp, List *target_list, IntUlongHashMap *phmiulOutputCols);

			// create the set operation given its children, input and output columns
			CDXLNode *CreateDXLSetOpFromColumns
				(
				EdxlSetOpType edxlsetop,
				List *plTargetListOutput,
				ULongPtrArray *pdrgpulOutput,
				ULongPtrArray2D *pdrgpdrgulInputColIds,
				DXLNodeArray *pdrgpdxlnChildren,
				BOOL fCastAcrossInput,
				BOOL fKeepResjunked
				)
				const;

			// check if the set operation need to cast any of its input columns
			BOOL SetOpNeedsCast(List *target_list, MdidPtrArray *pdrgpmdid) const;
			// translate a window operator
			CDXLNode *TranslateWindowToDXL
				(
				CDXLNode *pdxlnChild,
				List *target_list,
				List *plWindowClause,
				List *plSortClause,
				IntUlongHashMap *phmiulSortColsColId,
				IntUlongHashMap *phmiulOutputCols
				);

			// translate window spec
			DXLWindowSpecArray *TranslateWindowSpecToDXL(List *plWindowClause, IntUlongHashMap *phmiulSortColsColId, CDXLNode *pdxlnScPrL);

			// update window spec positions of LEAD/LAG functions
			void UpdateLeadLagWinSpecPos(CDXLNode *project_list_dxl, DXLWindowSpecArray *pdrgpdxlwinspec) const;

			// manufucture window frame for lead/lag functions
			CDXLWindowFrame *CreateWindowFramForLeadLag(BOOL fLead, CDXLNode *pdxlnOffset) const;

			// translate the child of a set operation
			CDXLNode *TranslateSetOpChild(Node *pnodeChild, ULongPtrArray *pdrgpul, MdidPtrArray *pdrgpmdid, List *target_list);

			// return a dummy const table get
			CDXLNode *DXLDummyConstTableGet() const;

			// translate an Expr into CDXLNode
			CDXLNode *TranslateExprToDXL(Expr *pexpr);

			// translate the JoinExpr (inside FromExpr) into a CDXLLogicalJoin node
			CDXLNode *TranslateJoinExprInFromToDXL(JoinExpr *pjoinexpr);

			// construct a group by node for a set of grouping columns
			CDXLNode *CreateSimpleGroupBy
				(
				List *target_list,
				List *plGroupClause,
				CBitSet *pbs,
				BOOL fHasAggs,
				BOOL fGroupingSets,				// is this GB part of a GS query
				CDXLNode *pdxlnChild,
				IntUlongHashMap *phmiulSortGrpColsColId,  // mapping sortgroupref -> ColId
				IntUlongHashMap *phmiulChild,				// mapping attno->colid in child node
				IntUlongHashMap *phmiulOutputCols			// mapping attno -> ColId for output columns
				);

			// check if the argument of a DQA has already being used by another DQA
			static
			BOOL IsDuplicateDqaArg(List *plDQA, Aggref *paggref);

			// translate a query with grouping sets
			CDXLNode *TranslateGroupingSets
				(
				FromExpr *pfromexpr,
				List *target_list,
				List *plGroupClause,
				BOOL fHasAggs,
				IntUlongHashMap *phmiulSortGrpColsColId,
				IntUlongHashMap *phmiulOutputCols
				);

			// expand the grouping sets into a union all operator
			CDXLNode *CreateDXLUnionAllForGroupingSets
				(
				FromExpr *pfromexpr,
				List *target_list,
				List *plGroupClause,
				BOOL fHasAggs,
				BitSetArray *pdrgpbsGroupingSets,
				IntUlongHashMap *phmiulSortGrpColsColId,
				IntUlongHashMap *phmiulOutputCols,
				UlongUlongHashMap *phmululGrpColPos		// mapping pos->unique grouping columns for grouping func arguments
				);

			// construct a project node with NULL values for columns not included in the grouping set
			CDXLNode *CreateDXLProjectNullsForGroupingSets
				(
				List *target_list, 
				CDXLNode *pdxlnChild, 
				CBitSet *pbs, 
				IntUlongHashMap *phmiulSortgrouprefCols, 
				IntUlongHashMap *phmiulOutputCols, 
				UlongUlongHashMap *phmululGrpColPos
				) 
				const;

			// construct a project node with appropriate values for the grouping funcs in the given target list
			CDXLNode *CreateDXLProjectGroupingFuncs
				(
				List *target_list,
				CDXLNode *pdxlnChild,
				CBitSet *pbs,
				IntUlongHashMap *phmiulOutputCols,
				UlongUlongHashMap *phmululGrpColPos,
				IntUlongHashMap *phmiulSortgrouprefColId
				)
				const;

			// add sorting and grouping column into the hash map
			void AddSortingGroupingColumn(TargetEntry *target_entry, IntUlongHashMap *phmiulSortGrpColsColId, ULONG col_id) const;

			// translate the list of sorting columns
			DXLNodeArray *TranslateSortColumsToDXL(List *plSortCl, IntUlongHashMap *phmiulColColId) const;

			// translate the list of partition-by column identifiers
			ULongPtrArray *TranslatePartColumns(List *plSortCl, IntUlongHashMap *phmiulColColId) const;

			CDXLNode *TranslateLimitToDXLGroupBy
				(
				List *plsortcl, // list of sort clauses
				Node *pnodeLimitCount, // query node representing the limit count
				Node *pnodeLimitOffset, // query node representing the limit offset
				CDXLNode *dxlnode, // the dxl node representing the subtree
				IntUlongHashMap *phmiulGrpColsColId // the mapping between the position in the TargetList to the ColId
				);

			// throws an exception when RTE kind not yet supported
			void UnsupportedRTEKind(RTEKind rtekind) const;

			// translate an entry of the from clause (this can either be FromExpr or JoinExpr)
			CDXLNode *TranslateFromClauseToDXL(Node *node);

			// translate the target list entries of the query into a logical project
			CDXLNode *TranslateTargetListToDXLProject
				(
				List *target_list,
				CDXLNode *pdxlnChild,
				IntUlongHashMap *	phmiulGrpcolColId,
				IntUlongHashMap *phmiulOutputCols,
				List *plGrpCl,
				BOOL fExpandAggrrefExpr = false
				);

			// translate a target list entry or a join alias entry into a project element
			CDXLNode *TranslateExprToDXLProject(Expr *pexpr, const CHAR *szAliasName, BOOL fInsistNewColIds = false);

			// translate a CTE into a DXL logical CTE operator
			CDXLNode *TranslateCTEToDXL
				(
				const RangeTblEntry *prte,
				ULONG ulRTIndex,
				ULONG ulCurrQueryLevel
				);

			// translate a base table range table entry into a logical get
			CDXLNode *TranslateRTEToDXLLogicalGet
				(
				const RangeTblEntry *prte,
				ULONG ulRTIndex,
				ULONG //ulCurrQueryLevel
				);

			// generate a DXL node from column values, where each column value is
			// either a datum or scalar expression represented as a project element.
			CDXLNode *TranslateColumnValuesToDXL
			 	(
			 	DXLDatumArray *pdrgpdxldatum,
			 	ColumnDescrDXLArray *pdrgpdxlcdCTG,
			 	DXLNodeArray *pdrgpdxlnPE
			    )
			    const;

			// translate a value scan range table entry
			CDXLNode *TranslateValueScanRTEToDXL
				(
				const RangeTblEntry *prte,
				ULONG ulRTIndex,
				ULONG //ulCurrQueryLevel
				);

			// create a dxl node from a array of datums and project elements
			CDXLNode *TranslateTVFToDXL
				(
				const RangeTblEntry *prte,
				ULONG ulRTIndex,
				ULONG //ulCurrQueryLevel
				);

			// translate a derived table into a DXL logical operator
			CDXLNode *TranslateDerivedTablesToDXL
						(
						const RangeTblEntry *prte,
						ULONG ulRTIndex,
						ULONG ulCurrQueryLevel
						);

			// create a DXL node representing the scalar constant "true"
			CDXLNode *CreateDXLConstValueTrue();

			// store mapping attno->colid
			void StoreAttnoColIdMapping(IntUlongHashMap *phmiul, INT iAttno, ULONG col_id) const;

			// construct an array of output columns
			DXLNodeArray *CreateDXLOutputCols(List *target_list, IntUlongHashMap *phmiulAttnoColId) const;

			// check for support command types, throws an exception when command type not yet supported
			void CheckSupportedCmdType(Query *query);

			// translate a select-project-join expression into DXL
			CDXLNode *TranslateSelectProjectJoinToDXL(List *target_list, FromExpr *pfromexpr, IntUlongHashMap *phmiulSortGroupColsColId, IntUlongHashMap *phmiulOutputCols, List *plGroupClause);

			// translate a select-project-join expression into DXL and keep variables appearing
			// in aggregates and grouping columns in the output column map
			CDXLNode *TranslateSelectProjectJoinForGrpSetsToDXL(List *target_list, FromExpr *pfromexpr, IntUlongHashMap *phmiulSortGroupColsColId, IntUlongHashMap *phmiulOutputCols, List *plGroupClause);
			
			// helper to check if OID is included in given array of OIDs
			static
			BOOL OIDFound(OID oid, const OID rgOID[], ULONG size);

			// check if given operator is lead() window function
			static
			BOOL IsLeadWindowFunc(CDXLOperator *GetOperator);

			// check if given operator is lag() window function
			static
			BOOL IsLagWindowFunc(CDXLOperator *dxlop);

		    // translate an insert query
			CDXLNode *TranslateInsertQueryToDXL();

			// translate a delete query
			CDXLNode *TranslateDeleteQueryToDXL();

			// translate an update query
			CDXLNode *TranslateUpdateQueryToDXL();

		    // translate a CTAS query
			CDXLNode *TranslateCTASToDXL();
			
			// translate CTAS storage options
			CDXLCtasStorageOptions::DXLCtasOptionArray *GetDXLCtasOptionArray(List *plOptions, IMDRelation::Erelstoragetype *perelstoragetype);
			
			// extract storage option value from defelem
			CWStringDynamic *ExtractStorageOptionStr(DefElem *pdefelem);
			
			// return resno -> colId mapping of columns to be updated
			IntUlongHashMap *UpdatedColumnMapping();

			// obtain the ids of the ctid and segmentid columns for the target
			// table of a DML query
			void GetCtidAndSegmentId(ULONG *pulCtid, ULONG *pulSegmentId);
			
			// obtain the column id for the tuple oid column of the target table
			// of a DML statement
			ULONG GetTupleOidColId();

			// translate a grouping func expression
			CDXLNode *TranslateGroupingFuncToDXL(const Expr *pexpr, CBitSet *pbs, UlongUlongHashMap *phmululGrpColPos) const;

			// construct a list of CTE producers from the query's CTE list
			void ConstructCTEProducerList(List *plCTE, ULONG query_level);
			
			// construct a stack of CTE anchors for each CTE producer in the given array
			void ConstructCTEAnchors(DXLNodeArray *pdrgpdxln, CDXLNode **ppdxlnCTEAnchorTop, CDXLNode **ppdxlnCTEAnchorBottom);
			
			// generate an array of new column ids of the given size
			ULongPtrArray *GenerateColIds(IMemoryPool *memory_pool, ULONG size) const;

			// extract an array of colids from the given column mapping
			ULongPtrArray *ExtractColIds(IMemoryPool *memory_pool, IntUlongHashMap *phmiul) const;
			
			// construct a new mapping based on the given one by replacing the colid in the "From" list
			// with the colid at the same position in the "To" list
			IntUlongHashMap *RemapColIds(IMemoryPool *memory_pool, IntUlongHashMap *phmiul, ULongPtrArray *pdrgpulFrom, ULongPtrArray *pdrgpulTo) const;

			// true iff this query or one of its ancestors is a DML query
			BOOL IsDMLQuery();

		public:
			// dtor
			~CTranslatorQueryToDXL();

			// query object
			const Query *Pquery() const
			{
				return m_pquery;
			}

			// does query have distributed tables
			BOOL HasDistributedTables() const
			{
				return m_fHasDistributedTables;
			}

			// main translation routine for Query -> DXL tree
			CDXLNode *TranslateSelectQueryToDXL();

			// main driver
			CDXLNode *TranslateQueryToDXL();

			// return the list of output columns
			DXLNodeArray *GetQueryOutputCols() const;

			// return the list of CTEs
			DXLNodeArray *GetCTEs() const;

			// factory function
			static
			CTranslatorQueryToDXL *QueryToDXLInstance
				(
				IMemoryPool *memory_pool,
				CMDAccessor *md_accessor,
				CIdGenerator *pidgtorColId,
				CIdGenerator *pidgtorCTE,
				CMappingVarColId *var_col_id_mapping,
				Query *query,
				ULONG query_level,
				HMUlCTEListEntry *phmulCTEEntries = NULL // hash map between query level -> list of CTEs defined at that level
				);
	};
}
#endif // GPDXL_CTranslatorQueryToDXL_H

//EOF
