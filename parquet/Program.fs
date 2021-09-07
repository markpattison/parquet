

open System
open System.Diagnostics
open System.Text.Json

open ParquetSharp

type MyClass(x: float[][], b: bool) =
    member val MyFloat = x with get, set
    member val MyBool = b with get, set
    new() = MyClass([| |], false)

let randomPoint r c =
    let arr1 = Array.init 17 (fun i -> 1.0 + float i)
    let arr2 = Array.init 17 (fun i -> sin (float i + Math.PI * float r + Math.PI / float (1 + c)))
    let bool = (r + c) % 2 = 0

    MyClass([| arr1; arr2 |], bool)

let randomData rows columns =
    Array2D.init rows columns randomPoint

let timed f x message =
    let stopWatch = Stopwatch()
    stopWatch.Start()
    let result = f x
    let elapsed = stopWatch.Elapsed.TotalSeconds
    printfn "%s: %.2f" message elapsed
    result

[<EntryPoint>]
let main argv =
    
    let nRows = 5000
    let nColumns = 120

    let serialize data =
        Array2D.map JsonSerializer.Serialize data

    let writeFile (rowIds: int[]) (data: string[,]) (filename: string) =
        let dataColumns : Column[] = Array.init nColumns (fun c -> Column<string>($"Column {c}") :> Column)
        let columns : Column[] = Array.append [| Column<int>("RowID") |] dataColumns

        use file = new ParquetFileWriter(filename, columns)
        use rowGroup = file.AppendRowGroup()

        use rowIdWriter = rowGroup.NextColumn().LogicalWriter<int>()
        rowIdWriter.WriteBatch(rowIds)

        for i in 0 .. (nColumns - 1) do
            use dataWriter = rowGroup.NextColumn().LogicalWriter<string>()
            dataWriter.WriteBatch(data.[*, i])

        file.Close()

    let readFile (filename: string) =
        use reader = new ParquetFileReader(filename)
        let metadata = reader.FileMetaData

        let rowGroup = reader.RowGroup(0)

        let results = Array.init nColumns (fun r ->
            let recordReader = rowGroup.Column(r + 1).LogicalReader<string>()
            let dest: string[] = Array.zeroCreate nRows
            recordReader.ReadBatch(dest, 0, nRows) |> ignore
            dest)

        results

    let deserialize data =
        Array.map (fun arr -> Array.map (fun (json: string) -> JsonSerializer.Deserialize<MyClass>(json)) arr) data

    let rowIds = Array.init nRows (fun r -> r + 1)
    let data = timed (randomData nRows) nColumns "Creating random data"
    let serialized = timed serialize data "Serializing data"
    timed (writeFile rowIds serialized) "output.parquet" "Writing parquet file"

    let jsonData = timed readFile "output.parquet" "Reading parquet file"
    let deserialized = timed deserialize jsonData "Deserializing"

    0 // return an integer exit code
