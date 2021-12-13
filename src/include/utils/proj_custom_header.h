typedef struct HistSlot {
    float8      lower;
    float8      upper;
    float8      value;
} HistSlot;

typedef struct CustomHist {
    HistSlot    *slots;
    float8      range_count;
    int         min;      
    int         max;      
} CustomHist;

typedef struct SimpleRange
{
    int      start;
    int      end;
    int      length;
} SimpleRange;

CustomHist *construct_hist(float8 *hist_bins, float8 *slots_values, int slots_count);
CustomHist *normalize_hist(CustomHist *hist, int new_min, int new_max, int slots_count);
float8 accumulate_range_in_slot_percentage(float8 s_min, float8 s_max, SimpleRange range);
float8 vectors_dot_product(HistSlot *slots_1, HistSlot *slots_2, int slots_count);