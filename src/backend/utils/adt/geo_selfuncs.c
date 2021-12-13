/*-------------------------------------------------------------------------
 *
 * geo_selfuncs.c
 *	  Selectivity routines registered in the operator catalog in the
 *	  "oprrest" and "oprjoin" attributes.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/geo_selfuncs.c
 *
 *	XXX These are totally bogus.  Perhaps someone will make them do
 *	something reasonable, someday.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/geo_decls.h"
#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/rangetypes.h"
#include "utils/proj_custom_header.h"

/*
 *	Selectivity functions for geometric operators.  These are bogus -- unless
 *	we know the actual key distribution in the index, we can't make a good
 *	prediction of the selectivity of these operators.
 *
 *	Note: the values used here may look unreasonably small.  Perhaps they
 *	are.  For now, we want to make sure that the optimizer will make use
 *	of a geometric index if one is available, so the selectivity had better
 *	be fairly small.
 *
 *	In general, GiST needs to search multiple subtrees in order to guarantee
 *	that all occurrences of the same key have been found.  Because of this,
 *	the estimated cost for scanning the index ought to be higher than the
 *	output selectivity would indicate.  gistcostestimate(), over in selfuncs.c,
 *	ought to be adjusted accordingly --- but until we can generate somewhat
 *	realistic numbers here, it hardly matters...
 */


/*
 * Selectivity for operators that depend on area, such as "overlap".
 */

Datum
areasel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

Datum
areajoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.005);
}

/*
 *	positionsel
 *
 * How likely is a box to be strictly left of (right of, above, below)
 * a given box?
 */

Datum
positionsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

Datum
positionjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.1);
}

/*
 *	contsel -- How likely is a box to contain (be contained by) a given box?
 *
 * This is a tighter constraint than "overlap", so produce a smaller
 * estimate than areasel does.
 */

Datum
contsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}

Datum
contjoinsel(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(0.001);
}

CustomHist *construct_hist(float8 *hist_bins, float8 *slots_values, int slots_count) {
    CustomHist *hist;
    
    float8      hist_min;
    float8      hist_max;
    float8      range_count = 0;
    
    HistSlot *slots = (HistSlot *) palloc(sizeof(HistSlot) * slots_count);

    for(int i = 0; i < slots_count; i++) {
        if(i == 0) {
            hist_min = slots[i].lower;
            hist_max = slots[i].upper;
        }

        slots[i].lower = hist_bins[i];
        slots[i].upper = hist_bins[i + 1];
        slots[i].value = slots_values[i];

        if(slots[i].lower < hist_min) hist_min = slots[i].lower;
        if(slots[i].upper > hist_max) hist_max = slots[i].upper;

        range_count += slots[i].value;
    }

    hist = (CustomHist *) palloc(sizeof(slots) + 3 * sizeof(float8));
    
    hist->range_count = range_count;
    hist->min = hist_min;
    hist->max = hist_max;
    hist->slots = slots;

    return hist;
}


CustomHist *normalize_hist(CustomHist *hist, int new_min, int new_max, int slots_count) {
    float8 *hist_bins = (float8 *) palloc(sizeof(float8) * slots_count + 1); 
    float8 *slots_values = (float8 *) palloc(sizeof(float8) * slots_count);

    float8 slot_length = (new_max - new_min) / slots_count;

    // initialize the new bins
    for(int i = 0; i <= slots_count; i++) {
        hist_bins[i] = (float8)(i * slot_length) + new_min;
        if(i == slots_count)
            hist_bins[i] = new_max;
    }

    // initialize the new slots
    for(int i = 0; i < slots_count; i++) {
        slots_values[i] = 0; // clean up the old stuff in memory
        
        float8 slot_min = hist_bins[i];
        float8 slot_max = hist_bins[i + 1];

        for(int j = 0; j < slots_count; j++) {
            SimpleRange curr_range;

            curr_range.start = hist->slots[j].lower;
            curr_range.end = hist->slots[j].upper;
            curr_range.length = curr_range.end - curr_range.start;
            
            float8 coverage = accumulate_range_in_slot_percentage(slot_min, slot_max, curr_range);
            slots_values[i] += coverage * hist->slots[j].value;
        }
    }

    return construct_hist(hist_bins, slots_values, slots_count);
}

float8 vectors_dot_product(HistSlot *slots_1, HistSlot *slots_2, int slots_count) {
    float8 total = 0;
    for(int i = 0; i < slots_count; i++) {
        total += slots_1[i].value * slots_2[i].value;
    }
    return total;
}


/*
 * Range Overlaps Join Selectivity.
 */
Datum
rangeoverlapsjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid         operator = PG_GETARG_OID(1);
    List       *args = (List *) PG_GETARG_POINTER(2);
    // JoinType    jointype = (JoinType) PG_GETARG_INT16(3);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid         collation = PG_GET_COLLATION();

    double      selec = 0.005;

    VariableStatData vardata1;
    VariableStatData vardata2;
    Oid         opfuncoid;
    
    AttStatsSlot sslot11;
    AttStatsSlot sslot12;
    AttStatsSlot sslot21;
    AttStatsSlot sslot22;
    
    int         bins_count1;
    int         bins_count2;

    // slots_count1 and slots_count2 should be equal tho
    int         slots_count1;
    int         slots_count2;

    CustomHist  *hist1;
    CustomHist  *hist2;

    Form_pg_statistic stats1 = NULL;
    Form_pg_statistic stats2 = NULL;
    TypeCacheEntry *typcache = NULL;
    
    bool        join_is_reversed;
    bool        empty;

    get_join_variables(root, args, sjinfo,
                       &vardata1, &vardata2, &join_is_reversed);

    typcache = range_get_typcache(fcinfo, vardata1.vartype);
    opfuncoid = get_opcode(operator);

    // make the first histogram
    memset(&sslot11, 0, sizeof(sslot11));
    memset(&sslot12, 0, sizeof(sslot12));
    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata1, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);
    if (HeapTupleIsValid(vardata1.statsTuple))
    {
        stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        if (!get_attstatsslot(&sslot11, vardata1.statsTuple,
                             STATISTIC_KIND_BINS_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot12, vardata1.statsTuple,
                             STATISTIC_KIND_BINS_VALUES_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
    }
    bins_count1 = sslot11.nvalues;  //number of bins
    slots_count1 = sslot12.nvalues;  //number of slots (#bins - 1)
    float8 *hist_bins1 = (float8 *) palloc(sizeof(float8) * bins_count1);
    float8 *slots_values1 = (float8 *) palloc(sizeof(float8) * slots_count1);
    for(int i = 0; i < bins_count1; i++) {
        hist_bins1[i] = DatumGetFloat8(sslot11.values[i]);
    }
    for(int i = 0; i < slots_count1; i++) {
        slots_values1[i] = DatumGetFloat8(sslot12.values[i]);
    }

    //make the second histogram
    memset(&sslot21, 0, sizeof(sslot21));
    memset(&sslot22, 0, sizeof(sslot22));
    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata2, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);
    if (HeapTupleIsValid(vardata1.statsTuple))
    {
        stats2 = (Form_pg_statistic) GETSTRUCT(vardata2.statsTuple);
        if (!get_attstatsslot(&sslot21, vardata2.statsTuple,
                             STATISTIC_KIND_BINS_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot22, vardata2.statsTuple,
                             STATISTIC_KIND_BINS_VALUES_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
    }
    bins_count2 = sslot21.nvalues;  //number of bins
    slots_count2 = sslot22.nvalues;  //number of slots (#bins - 1)
    float8 *hist_bins2 = (float8 *) palloc(sizeof(float8) * bins_count2);
    float8 *slots_values2 = (float8 *) palloc(sizeof(float8) * slots_count2);
    for(int i = 0; i < bins_count2; i++) {
        hist_bins2[i] = DatumGetFloat8(sslot21.values[i]);
    }
    for(int i = 0; i < slots_count2; i++) {
        slots_values2[i] = DatumGetFloat8(sslot22.values[i]);
    }


    hist1 = construct_hist(hist_bins1, slots_values1, slots_count1);
    hist2 = construct_hist(hist_bins2, slots_values2, slots_count1);
    
    int common_min = Max(hist1->min, hist2->min);
    int common_max = Min(hist1->max, hist2->max);

    CustomHist *norm_1 = normalize_hist(hist1, common_min, common_max, slots_count1);
    CustomHist *norm_2 = normalize_hist(hist2, common_min, common_max, slots_count2);

    float dp = vectors_dot_product(norm_1->slots, norm_2->slots, slots_count1);
    selec = dp / (hist1->range_count * hist2->range_count);

    printf("selc: %f", selec);

    fflush(stdout);

    free_attstatsslot(&sslot11);
    free_attstatsslot(&sslot12);
    free_attstatsslot(&sslot21);
    free_attstatsslot(&sslot22);

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}
