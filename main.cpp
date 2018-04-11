#include <iostream>
#include <fstream>
#include "proto/uds.pb.h"
#include "clickhouse/client.h"

static std::string Decode(const std::string &input, std::string &out) {
    static constexpr unsigned char kDecodingTable[] = {
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
            52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
            64, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
            64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
            64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64
    };

    size_t in_len = input.size();
    if (in_len % 4 != 0) return "Input data size is not a multiple of 4";

    size_t out_len = in_len / 4 * 3;
    if (input[in_len - 1] == '=') out_len--;
    if (input[in_len - 2] == '=') out_len--;

    out.resize(out_len);

    for (size_t i = 0, j = 0; i < in_len;) {
        uint32_t a = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];
        uint32_t b = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];
        uint32_t c = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];
        uint32_t d = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];

        uint32_t triple = (a << 3 * 6) + (b << 2 * 6) + (c << 1 * 6) + (d << 0 * 6);

        if (j < out_len) out[j++] = (triple >> 2 * 8) & 0xFF;
        if (j < out_len) out[j++] = (triple >> 1 * 8) & 0xFF;
        if (j < out_len) out[j++] = (triple >> 0 * 8) & 0xFF;
    }

    return "";
}


int main(int argc, char* argv[]) {
        std::cout << "Start" << std::endl;

        clickhouse::Client client(
                clickhouse::ClientOptions()
                        .SetHost("clickhouseuds1.mlan")
                        .SetUser("krash")
                        .SetPassword("krashclick")
                        .SetCompressionMethod(clickhouse::CompressionMethod::LZ4)
                        .SetPingBeforeQuery(false)
        );

        for(int i =0; i<3;i++)
        {
            const clock_t begin_time = clock();
            clickhouse::Block block;
            auto dt = std::make_shared<clickhouse::ColumnDate>();
            auto ts = std::make_shared<clickhouse::ColumnDateTime>();
            auto event_name = std::make_shared<clickhouse::ColumnString>();
            auto batch_id = std::make_shared<clickhouse::ColumnString>();
            auto position = std::make_shared<clickhouse::ColumnUInt32>();
            auto cnt = std::make_shared<clickhouse::ColumnInt32>();

            auto key_arr = std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnString>());
            auto value_arr = std::make_shared<clickhouse::ColumnArray>(std::make_shared<clickhouse::ColumnString>());

            std::string out;

            const std::basic_string<char> &batch_id_value = std::string("local_uds_debug_gpb-2018-04-10_008596");
            std::ifstream input( argv[1] );

            for( std::string line; getline( input, line ); )
            {
                Decode(line, out);

                uds_event event;
                event.ParseFromString(out);

                dt->Append((time_t)event.ts());
                ts->Append((time_t)event.ts());
                event_name->Append(event.event_name());

                batch_id->Append(batch_id_value);
                position->Append(0);
                cnt->Append(event.count());

                auto keys = std::make_shared<clickhouse::ColumnString>();
                auto vals = std::make_shared<clickhouse::ColumnString>();
                for(int k = 0; k<event.keys_size();k++)
                {
                    keys->Append(event.keys(k));
                    vals->Append(event.values(k));
                }
                key_arr->AppendAsColumn(keys);
                value_arr->AppendAsColumn(vals);
            }

            std::cout << float(clock() - begin_time) / CLOCKS_PER_SEC << " READ" << std::endl;

            block.AppendColumn("dt", dt);
            block.AppendColumn("ts", ts);
            block.AppendColumn("event_name", event_name);
            block.AppendColumn("batch_id", batch_id);
            block.AppendColumn("position", position);
            block.AppendColumn("cnt", cnt);
            block.AppendColumn("keys", key_arr);
            block.AppendColumn("vals", value_arr);


            std::cout << float(clock() - begin_time) / CLOCKS_PER_SEC << " START" << std::endl;
            client.Insert("default.krash_uds", block);

            std::cout << float(clock() - begin_time) / CLOCKS_PER_SEC << " FINISH" <<  std::endl;
        }



    return 0;
}
