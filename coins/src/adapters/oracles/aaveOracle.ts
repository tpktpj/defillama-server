import { getCurrentUnixTimestamp } from "../../utils/date";
import { addToDBWritesList } from "../utils/database";
import { Write } from "../utils/dbInterfaces";
import { getTokenInfo } from "../utils/erc20";
import { getApi } from "../utils/sdk";

const now = getCurrentUnixTimestamp();

const contracts: { [chain: string]: { target: string; queries: string[] } } = {
  btr: {
    target: "0x191a6ac7cbC29De2359de10505E05935a1Ed5478", // AaveOracle-Bitlayer
    queries: [
        "0xb88a54ebbda8edbc1c2816ace1dc2b7c6715972d",
        "0xb750f79cf4768597f4d05d8009fcc7cee2704824"
    ],
  },
};

export async function aaveOracle() {
  const writes: Write[] = [];

  await Promise.all(
    Object.keys(contracts).map(async (chain: string) => {
      const api = await getApi(chain, now);
      const { target, queries } = contracts[chain];
      const calls = queries.map((params: string) => ({ params }));
      const [baseCurrencyUnit, assetPrice, metadata] = await Promise.all([
        api.call({ target, abi: "function baseCurrencyUnit() view returns (uint256)" }),
        api.multiCall({
          target,
          abi: "function getAssetPrice(address) view returns (uint256)",
          calls,
        }),
        getTokenInfo(chain, queries, undefined),
      ]);

    const timestamp = now;

      assetPrice.forEach((r, i) => {
        addToDBWritesList(
          writes,
          chain,
          queries[i],
          r / baseCurrencyUnit,
          metadata.decimals[i].output,
          metadata.symbols[i].output,
          timestamp,
          "aaveOracle",
          0.95,
        );
      });
    }),
  );

  return writes;
}
