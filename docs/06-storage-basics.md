# Storage Basics

This article explains the important types/traits at the storage layer. 

## Types and traits

### Array & ArrayBuilder

Data is stored in columns in RisingLight. Here a column is referred to as an "array".  `Array` is a trait over all arrays, specifyng the interface for all arrays. `PrimitiveArray<T>` implements the `Array` trait, where `T` can be `bool`, `i32`, etc. `ArrayImpl` is an enum over different arrays.

`ArrayBuilder` is a trait over array builders that builds `Array`. Similar to how `PrimitiveArray<T>` implements `Array`, `PrimitiveArrayBuilder<T>` implements `ArrayBuilder`. `ArrayBuilderImpl` is an enum over different array builders.

Some fields/methods are shown below:

```rust
pub trait Array: Sized + Send + Sync + 'static {
    /// Corresponding builder of this array.
    type Builder: ArrayBuilder<Array = Self>;
    /// Type of element in the array.
    type Item: ToOwned + ?Sized;

    /// Retrieve a reference to value.
    fn get(&self, idx: usize) -> Option<&Self::Item>;

    /// Get iterator of current array.
    fn iter(&self) -> ArrayIter<'_, Self> {
        ArrayIter::new(self)
    }

    ...
}

pub trait ArrayBuilder: Sized + Send + Sync + 'static {
    /// Corresponding `Array` of this builder
    type Array: Array<Builder = Self>;

    /// Append a value to builder.
    fn push(&mut self, value: Option<&<Self::Array as Array>::Item>);

    /// Take all elements and return a new array.
    fn take(&mut self) -> Self::Array;

    /// Finish build and return a new array.
    fn finish(mut self) -> Self::Array {
        self.take()
    }
    
    ...
}
```

### DataChunk & DataChunkBuilder

`DataChunk` is a list of arrays, representing a contiguous set of rows. 

`DataChunkBuilder` uses a list of array builders to build data chunks.

```rust
pub struct DataChunk {
    arrays: Arc<[ArrayImpl]>,
    ...
}

pub struct DataChunkBuilder {
    array_builders: Vec<ArrayBuilderImpl>,
    ...
}

impl DataChunkBuidler {
    // Add a row (a list of DataValue's) to the data chunk
    fn push_row(&mut self, row: impl IntoIterator<Item = DataValue>) -> Option<DataChunk>;
    
    // Finishes building data chunk
    fn take(&mut self) -> Option<DataChunk>;
}

```

### MemTable

Each transaction has an in-memory table as a write buffer.

The `MemTable` trait specifies that the mem-table should take `DataChunk`s and outputs a `DataChunk`.

```rust
pub trait MemTable {
    /// add data to memory table
    fn append(&mut self, columns: DataChunk) -> StorageResult<()>;

    /// flush data to [`DataChunk`]
    fn flush(self) -> StorageResult<DataChunk>;
}

pub struct BTreeMapMemTable {
    columns: Arc<[ColumnCatalog]>,
    primary_key_idx: usize,
    multi_btree_map: BTreeMultiMap<ComparableDataValue, Row>, // <primary key, row data>
}
```

`BTreeMapMemTable` implements `MemTable`. At `append`, it adds each row in the data chunk to its internal B-tree map. At `flush`, it creates array builders using type information from `ColumnCatalog`, builds arrays from rows using those builders, and constructs a `DataChunk` from those arrays.

### SecondaryMemRowSet

`SecondaryMemRowsetImpl` represents a mem-table that flushes to disk. Those flushed data will be in `EncodedRowset` format.

```rust
pub struct SecondaryMemRowset<M: MemTable> {
    mem_table: M,
    rowset_id: u32,
    rowset_builder: RowsetBuilder,
}

impl SecondaryMemRowset<BTreeMapMemTable> {
    pub fn append(&mut self, columns: DataChunk) -> StorageReulst<()> {
        self.mem_table.append(columns)
    }
    
    pub fn flush(self, ..) -> StorageResult<()> {
        let chunk = self.mem_table.flush();
		
        self.rowset_builder.append(chunk);
        let encoded_rowset = self.rowset_builder.finish();
        
        let writer = RowsetWriter::new();
        writer.flush(encoded_rowset);
    }
}

pub enum SecondaryMemRowsetImpl {
    BTree(SecondaryMemRowset<BTreeMapMemTable>),
    Column(SecondaryMemRowset<ColumnMemTable>),
}
```

### Columns & Blocks

An `EncodedColumn` is a column in serialized format, containing both index and data of the column in binary.

Column builders, defined by the `ColumnBuilder` trait, return `(Vec<BlockIndex>, Vec<u8>)`  when finishing a column. Note the returned index (`Vec<u8>`) is not in binary format yet and `IndexBuilder` serializes `Vec<BlockIndex>` to `Vec<u8>` later. 

```rust
pub struct EncodedColumn {
	index: Vec<u8>,
    data: Vec<u8>
}

pub trait ColumnBuilder<A: Array> {
    /// Append an [`Array`] to the column. [`ColumnBuilder`] will chunk it into small parts.
    fn append(&mut self, array: &A);

    /// Finish a column, return (index, data) for the block
    fn finish(self) -> (Vec<BlockIndex>, Vec<u8>);
}
```

`PrimitiveColumnBuilder<T>` implements `ColumnBuilder` trait. Internally, `PrimitiveColumnBuilder` rechunks data into `Block`s using `BlockBuilder`s. A `Block` is the minimum operation unit of secondary storage. Details about blocks are omitted here.

```rust
pub struct PrimitiveColumnBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    
    /// Current block builder
    current_builder: Option<BlockBuilderImpl<T>>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,

	...
}
```

`ColumnBuilderImpl` is an enum of different `PrimitiveColumnBuilder`s.

```rust
impl ColumnBuilderImpl {
    pub fn new_from_datatype(datatype: &DataType, options: ColumnBuilderOptions) -> Self;
    pub fn append(&mut self, array: &ArrayImpl);
    pub fn finish(self) -> (Vec<BlockIndex>, Vec<u8>);
}
```

### EncodedRowset & RowsetBuilder

An `EncodedRowset` is a collection of `EncodedColumns`. `RowsetBuilder` builds it using column builders. 

```rust
pub struct EncodedRowset {
 	columns_info: Arc<[ColumnCatalog]>,
	columns: Vec<EncodedColumn>,
    ...
}

pub struct RowsetBuilder {
    columns: Arc<[ColumnCatalog]>,
    builders: Vec<ColumnBuilderImpl>,
    ...
}

impl RowsetBuilder {
    fn append(&mut self, chunk: DataChunk) {
        for idx in 0..chunk.column_count() {
            self.builders[idx].append(chunk.array_at(idx));
        }
    }
    
    pub fn finish(self) -> EncodedRowset {
        EncodedRowset {
            columns_info: self.columns.clone(),
            columns: self
              .builders
              .into_iter()
              .map(|builder| {
              	  let (block_indices, data) = builder.finish();
                  
                  let mut index_builder = IndexBuilder::new(...);
                  for index in block_indices {
                      index_builder.append(index);
                  }
                  let index = index_builder.finish();
                  
                  EncodedColumn { index, data }
              })
              .collect_vec(),
        }
    }
}
```

### Transaction: Flush to Secondary Storage

A `SecondaryTransaction` represents a transaction that persists data to disk. A transaction buffers the writes (e.g. insertions) in the mem-table and only flushes to disk when (1) the mem-table exceeds a certain size threshold, or (2) the transaction commits.

```rust
pub struct SecondaryTransaction {
	mem: Option<SecondaryMemRowsetImpl>,
	...
}

impl SecondaryTransaction {
    fn flush_rowset(&mut self) {
        self.mem.flush();
		Add flushed rowset to self.to_be_committed_rowsets;
    }
    
    pub fn append(&mut self, columns: DataChunk) {
        if self.mem.is_none() {
			self.mem = ...
        }
        
        self.mem.append(columns);
        
        if memtable_size_exceeds_threshold() {
            self.flush_rowset();
        }
    }
    
   	pub fn commit(mut self) -> StorageResult<()> {
        self.flush_rowset();
        
        // Skipped: Flush DVs, Commit changeset to manifests, etc.
    }
}
```

### Summary

- `Array` represents a column in memory.
- `DataChunk` represents a contiguous set of rows, implemented as a collection of `Array`'s.
- `MemTable` is an in-memory write buffer. `append` takes `DataChunk`. `flush` outputs `DataChunk`.
- `SecondaryMemRowset<M: MemTable>` is a mem-table that flushes to secondary storage. `append` appends to its `MemTable`. `flush` flushes the `DataChunk` returned by `MemTable` to disk as `EncodedRowset`.
- `EncodedColumn` represents a column to be persisted to the secondary storage. It contains the index and data of a column. `ColumnBuilder` takes array at `append`, and outputs `EncodedColumn` at `flush`, roughly.
- `EncodedRowset` is a collection of `EncodedColumn`s. `RowsetBuilder` uses column builders internally. `RowsetBuilder` takes `DataChunk` at `append`, and outputs `EncodedRowset` at `flush`.

![types and traits](images/06-storage-basics-01.svg)

## `INSERT` execution

Let's walk through the whole process of executing `INSERT INTO table VALUES (1, '11')`:

- `VALUES (1, '11')` is stored as a `DataChunk` and passed to the insert executor. 
- The transaction adds the `DataChunk` to its memory-table.
- Whenever the mem-table is flushed (exceeding threshold/commit), a new `DataChunk` is produced. 
- The `DataChunk` is passed to `RowsetBuilder`.
- When the `RowsetBuilder` finishes, it produces an `EncodedRowset`, which is persisted to disk.

![types and traits](images/06-storage-basics-02.svg)



