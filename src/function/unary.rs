// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
pub struct UnaryExecutor;

impl UnaryExecutor {
    /// The unary function will only be executed when value is not null
    pub fn eval_batch_standard<I1, O, F>(
        i1: &ArrayImpl,
        mut f: F,
    ) -> Result<ArrayImpl, FunctionError>
    where
        I1: ArrayValidExt,
        for<'a> &'a I1: TryFrom<&'a ArrayImpl, Error = TypeMismatch>,
        O: Array + Into<ArrayImpl> + ArrayFromDataExt,
        <O::Item as ToOwned>::Owned: Default + Clone,
        F: for<'a> FnMut(&'a I1::Item, &'a mut FunctionCtx) -> <O::Item as ToOwned>::Owned,
    {
        let i1a: &I1 = i1.try_into().unwrap();
        let masks = i1a.get_valid_bitmap().clone();

        let mut ctx = FunctionCtx { error: None };

        let zeros = masks.count_zeros();
        if zeros == 0 {
            // all valid
            let i1a_raw_iter = i1a.raw_iter();
            // auto vectoried
            let res_iter = i1a_raw_iter.map(|x| f(x, &mut ctx));

            let res = O::from_data(res_iter, masks).into();

            if let Some(error) = ctx.error {
                return Err(error);
            }

            return Ok(res);
        }

        let mut builder: O::Builder = O::Builder::with_capacity(i1.len());

        if zeros == masks.len() {
            // all invalid
            // we update raw data and ignore valid-bitvec
            // replace valid-bitvec of builder with mask later
            builder.extend_from_nulls(zeros);
        } else {
            // a temporary buffer to combine our write operation
            let mut buffer = Vec::<<O::Item as ToOwned>::Owned>::with_capacity(64);
            buffer.resize(64, <O::Item as ToOwned>::Owned::default());
            // Process 64 pieces of data at a time,
            let mut cnt = 0;
            let mut mask_chunks = masks.chunks_exact(64);
            for y in mask_chunks.by_ref() {
                let zeros = y.count_zeros();
                if zeros == 0 {
                    // all valid
                    let base = cnt;
                    // auto vectoried
                    buffer
                        .iter_mut()
                        .enumerate()
                        .for_each(|(i, value)| *value = f(i1a.get_unchecked(base + i), &mut ctx));
                    builder.extend_from_raw_data(&buffer);
                } else if zeros == y.len() {
                    // all invalid
                    builder.extend_from_nulls(64);
                } else {
                    let base = cnt;
                    let mut res_count = 0;
                    buffer.iter_mut().enumerate().for_each(|(i, value)| unsafe {
                        if *masks.get_unchecked(base + i) {
                            *value = f(i1a.get_unchecked(base + i), &mut ctx);
                            res_count += 1;
                        }
                    });

                    builder.extend_from_raw_data(&buffer[0..res_count]);
                }
                cnt += 64;
            }

            let mut res_count = 0;
            buffer
                .iter_mut()
                .zip(mask_chunks.remainder().iter().by_ref())
                .enumerate()
                .for_each(|(i, (value, mask))| {
                    if *mask {
                        *value = f(i1a.get_unchecked(cnt + i), &mut ctx);
                        res_count += 1;
                    }
                });
            builder.extend_from_raw_data(&buffer[0..res_count]);
        }

        if let Some(error) = ctx.error {
            return Err(error);
        }

        builder.replace_bitmap(masks);
        Ok(builder.finish().into())
    }

    /// The null selection operation is performed after the unary function is executed
    pub fn eval_batch_lazy_select<I1, O, F>(
        l: &ArrayImpl,
        mut f: F,
    ) -> Result<ArrayImpl, FunctionError>
    where
        I1: ArrayValidExt,
        O: ArrayFromDataExt + Into<ArrayImpl>,
        <O::Item as ToOwned>::Owned: NativeType,
        for<'a> &'a I1: TryFrom<&'a ArrayImpl, Error = TypeMismatch>,
        F: for<'a> FnMut(&'a I1::Item, &'a mut FunctionCtx) -> <O::Item as ToOwned>::Owned,
    {
        let i1a: &I1 = l.try_into().unwrap();

        let mut ctx = FunctionCtx { error: None };

        let i1a_raw_iter = i1a.raw_iter();

        let res_iter = i1a_raw_iter.map(|x| f(x, &mut ctx));

        let res_mask = i1a.get_valid_bitmap().clone();

        let res = O::from_data(res_iter, res_mask).into();

        if let Some(error) = ctx.error {
            return Err(error);
        }

        Ok(res)
    }
}
