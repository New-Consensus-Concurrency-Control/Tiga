#include "multi_value.h"

bool operator<(const MultiValue &mv1, const MultiValue &mv2) {
   return mv1.compare(mv2) == -1;
}

Marshal &operator<<(Marshal &m, const MultiValue &mv) {
   m << mv.size();
   for (int i = 0; i < mv.size(); i++) {
      m << mv[i];
   }
   return m;
}

Marshal &operator>>(Marshal &m, MultiValue &mv) {
   int size;
   m >> size;
   MultiValue result(size);
   for (int i = 0; i < size; i++) {
      m >> result[i];
   }
   mv = result;
   return m;
}