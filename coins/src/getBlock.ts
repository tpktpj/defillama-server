import { successResponse, wrap, IResponse, errorResponse } from "./utils/shared";
import ddb, { getHistoricalValues } from "./utils/shared/dynamodb";
import { getProvider } from "@defillama/sdk/build/general"
import fetch from "node-fetch"
import { getCurrentUnixTimestamp } from "./utils/date";
import genesisBlockTimes from './genesisBlockTimes';
import { sendMessage } from "../../defi/src/utils/discord";

interface TimestampBlock {
  height: number;
  timestamp: number;
}

function cosmosBlockProvider(chain: "terra" | "kava") {
  return {
    getBlock: async (height: number | "latest") =>
      fetch(`${chain === "kava" ? "https://api.data.kava.io/blocks/" : "https://lcd.terra.dev/blocks/"}${height}`)
        .then((res) => res.json())
        .then((block) => ({
          number: Number(block.block.header.height),
          timestamp: Math.round(Date.parse(block.block.header.time) / 1000),
        })),
  };
}

function zkSyncBlockProvider() {
  return {
    getBlock: async (height: number | "latest") =>
      fetch(
        `https://api.zksync.io/api/v0.1/blocks${
          height == "latest" ? "" : `/${height}`
        }`,
      )
        .then((res) => res.json())
        .then((blocks) => ({
          number: Number(
            height == "latest" ? blocks[0].block_number : blocks.block_number,
          ),
          timestamp: Math.round(
            Date.parse(
              height == "latest" ? blocks[0].committed_at : blocks.committed_at,
            ) / 1000,
          ),
        })),
  };
}

export const blockPK = (chain: string) => `block#${chain}`

async function getBlock(provider: any, height: number | "latest", chain: string): Promise<TimestampBlock | IResponse> {
  const block = await provider.getBlock(height)
  if (block === null) {
    return errorResponse({ message: `Can't get block of chain ${chain} at height "${height}"`})
  }
  const previous = await getClosestBlock(blockPK(chain), block.timestamp - 1, 'low')
  if (previous && block.number < previous.height && block.timestamp > previous.timestamp) {
    await sendMessage(
      `${chain} block ${block.number} failed with timestamp ${block.timestamp}: id: ${provider.chainId}, string: ${provider.getBlock.toString()}`,
      process.env.STALE_COINS_ADAPTERS_WEBHOOK!,
      true,
    );
    return errorResponse({ message: `failed to getBlock`})
  }
  await ddb.put({
    PK: blockPK(chain),
    SK: block.timestamp,
    height: block.number,
  })
  return {
    height: block.number,
    timestamp: block.timestamp
  }
}

function getExtraProvider(chain: string | undefined) {
  if (chain === "terra" || chain === "kava") {
    return cosmosBlockProvider(chain)
  }
  if (["lite"].includes(chain as any)) return zkSyncBlockProvider();
  return getProvider(chain as any);
}

function isAValidBlockAtThisTimestamp(timestamp: number, chain: string) {
  if(timestamp > Date.now() / 1000){
    return false
  }
  if(genesisBlockTimes[chain]){
    return genesisBlockTimes[chain] < timestamp
  }
  return true
}

function getClosestBlock(PK: string, timestamp: number, search: "high" | "low") {
  return ddb
    .query({
      ExpressionAttributeValues: {
        ":pk": PK,
        ":timestamp": timestamp,
      },
      KeyConditionExpression: `PK = :pk AND SK ${search === "high" ? ">=" : "<="} :timestamp`,
      Limit: 1,
      ScanIndexForward: search === "high",
    })
    .then((records) => {
      const item = records.Items?.[0]
      return item === undefined ? undefined : {
        height: item.height,
        timestamp: item.SK
      } as TimestampBlock
    });
}

const handler = async (
  event: any
): Promise<IResponse> => {
  const { chain, timestamp: timestampRaw } = event.pathParameters!
  const provider = getExtraProvider(chain)
  if (provider === undefined || chain === undefined  || provider == null) {
    return errorResponse({
      message: "We don't support the blockchain we provided, make sure to spell it correctly"
    })
  }
  const timestamp = Number(timestampRaw)
  if (Number.isNaN(timestamp)) {
    return errorResponse({
      message: "Timestamp needs to be a number"
    })
  }
  const isValid = isAValidBlockAtThisTimestamp(timestamp, chain);
  if (!isValid)
    return errorResponse({
      message: `requested timestamp is either before genesis or after now`,
    });
  let [top, bottom] = await Promise.all([
    getClosestBlock(blockPK(chain), timestamp, "high"),
    getClosestBlock(blockPK(chain), timestamp, "low")
  ])
  if (top === undefined) {
    const topOrError = await getBlock(provider as any, "latest", chain);
    if ('body' in topOrError) return topOrError as IResponse
    else top = topOrError as TimestampBlock
    const currentTimestamp = getCurrentUnixTimestamp()
    if ((top.timestamp - currentTimestamp) < -30 * 60) {
      throw new Error(`Last block of chain "${chain}" is further than 30 minutes into the past`)
    }
  }
  if (bottom == undefined) {
    bottom = {
      height: chain === "terra" ? 4724001 : 0,
      timestamp: 0
    }
  }

  let high = top.height;
  let low = bottom.height;
  let block = top;
  while ((high - low) > 1) {
    const mid = Math.floor((high + low) / 2);
    const blockOrError = await getBlock(provider as any, mid, chain);
    if ('body' in block) return blockOrError as IResponse
    else block = blockOrError as TimestampBlock
    if (block.timestamp < timestamp) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  };
  if (Math.abs(block.timestamp - timestamp) > 3600) {
    throw new Error("Block selected is more than 1 hour away from the requested timestamp")
  }
  return successResponse(block);
}

export default wrap(handler);
