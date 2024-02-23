// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2024 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::ops::{Deref, DerefMut};

use nautilus_core::{ffi::cvec::CVec, time::UnixNanos};

use crate::{
    data::{delta::OrderBookDelta, deltas::OrderBookDeltas},
    enums::BookAction,
    identifiers::instrument_id::InstrumentId,
};

/// Provides a C compatible Foreign Function Interface (FFI) for an underlying [`OrderBookDeltas`].
///
/// This struct wraps `OrderBookDeltas` in a way that makes it compatible with C function
/// calls, enabling interaction with `OrderBookDeltas` in a C environment.
///
/// It implements the `Deref` trait, allowing instances of `OrderBookDeltas_API` to be
/// dereferenced to `OrderBookDeltas`, providing access to `OrderBookDeltas`'s methods without
/// having to manually access the underlying `OrderBookDeltas` instance.
#[repr(C)]
#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub struct OrderBookDeltas_API(Box<OrderBookDeltas>);

impl OrderBookDeltas_API {
    #[must_use]
    pub fn new(deltas: OrderBookDeltas) -> Self {
        Self(Box::new(deltas))
    }
}

impl Deref for OrderBookDeltas_API {
    type Target = OrderBookDeltas;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OrderBookDeltas_API {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Creates a new `OrderBookDeltas` object from a `CVec` of `OrderBookDelta`.
///
/// # Safety
/// - The `deltas` must be a valid pointer to a `CVec` containing `OrderBookDelta` objects
/// - This function clones the data pointed to by `deltas` into Rust-managed memory, then forgets the original `Vec` to prevent Rust from auto-deallocating it
/// - The caller is responsible for managing the memory of `deltas` (including its deallocation) to avoid memory leaks
#[no_mangle]
pub extern "C" fn orderbook_deltas_new(
    instrument_id: InstrumentId,
    deltas: &CVec,
) -> OrderBookDeltas_API {
    let CVec { ptr, len, cap } = *deltas;
    let deltas: Vec<OrderBookDelta> =
        unsafe { Vec::from_raw_parts(ptr.cast::<OrderBookDelta>(), len, cap) };
    let cloned_deltas = deltas.clone();
    std::mem::forget(deltas); // Prevents Rust from dropping `deltas`
    OrderBookDeltas_API::new(OrderBookDeltas::new(instrument_id, cloned_deltas))
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_drop(deltas: OrderBookDeltas_API) {
    drop(deltas); // Memory freed here
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_instrument_id(deltas: &OrderBookDeltas_API) -> InstrumentId {
    deltas.instrument_id
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_vec_deltas(deltas: &OrderBookDeltas_API) -> CVec {
    deltas.deltas.clone().into()
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_is_snapshot(deltas: &OrderBookDeltas_API) -> u8 {
    u8::from(deltas.deltas[0].action == BookAction::Clear)
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_flags(deltas: &OrderBookDeltas_API) -> u8 {
    deltas.flags
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_sequence(deltas: &OrderBookDeltas_API) -> u64 {
    deltas.sequence
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_ts_event(deltas: &OrderBookDeltas_API) -> UnixNanos {
    deltas.ts_event
}

#[no_mangle]
pub extern "C" fn orderbook_deltas_ts_init(deltas: &OrderBookDeltas_API) -> UnixNanos {
    deltas.ts_init
}

#[allow(clippy::drop_non_drop)]
#[no_mangle]
pub extern "C" fn orderbook_deltas_vec_drop(v: CVec) {
    let CVec { ptr, len, cap } = v;
    let deltas: Vec<OrderBookDelta> =
        unsafe { Vec::from_raw_parts(ptr.cast::<OrderBookDelta>(), len, cap) };
    drop(deltas); // Memory freed here
}
