defmodule CompressionTest do
  use ExUnit.Case

  test "snappy decompression works with chunked messages" do
    data =
      <<130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 14, 12,
        44, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 3, 246, 0, 0, 0, 75, 246, 7, 92, 10,
        44, 16, 236, 0, 0, 255, 255, 255, 255, 0, 0, 3, 232, 65, 66, 67, 68, 69,
        70, 71, 72, 73, 74, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254,
        10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254,
        10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 118, 10, 0>>

    expected =
      <<0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 3, 246, 10, 44, 16, 236, 0, 0, 255, 255,
        255, 255, 0, 0, 3, 232>> <> String.duplicate("ABCDEFGHIJ", 100)

    assert expected == KafkaEx.Compression.decompress(2, data)
  end

  describe "lz4" do
    test "decompress already compressed file" do
      data = Path.join(__DIR__, "hound.txt") |> File.read!()

      {:ok, compressed_data} =
        Path.join(__DIR__, "hound.txt.lz4") |> File.read()

      decompressed_data = KafkaEx.Compression.decompress(3, compressed_data)

      assert data == decompressed_data
    end

    test "data |> compress |> decompress == data" do
      data = Path.join(__DIR__, "hound.txt") |> File.read!()

      {compressed_data, 3} = KafkaEx.Compression.compress(:lz4, data)

      decompressed_data = KafkaEx.Compression.decompress(3, compressed_data)

      assert byte_size(compressed_data) < byte_size(decompressed_data)

      assert decompressed_data == data
    end
  end
end
