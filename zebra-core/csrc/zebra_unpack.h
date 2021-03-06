#ifndef __ZEBRA_UNPACK_H
#define __ZEBRA_UNPACK_H

#include "zebra_data.h"

error_t zebra_unpack_array (
    uint8_t *bytes
  , int64_t bufsize
  , int64_t elems
  , int64_t offset
  , int64_t *fill
  );

error_t zebra_pack_array (
    uint8_t **buf_inout
  , int64_t *elems
  , int64_t n_elems
  );

#endif//__ZEBRA_UNPACK_H

