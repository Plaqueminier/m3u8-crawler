import { compact } from "lodash";
import logger from "./logger";

const list: { username: string; rank: number }[] = [
];

const listNotInApi: string[] = [];

interface ApiType {
  username: string;
  num_users: number;
  current_show: string;
}

interface OutputType {
  username: string;
  numUsers: number;
  currentShow: string;
}

export const findPerson = async (
  index: number
): Promise<string | undefined> => {
  const infos = await fetch(process.env.API_URL ?? "https://api.example.com");
  const infosNotInApi = compact(
    await Promise.all(
      listNotInApi.map(async (name) => {
        const res = await (
          await fetch(
            `${
              process.env.API_URL_NOT_IN_API ?? "https://api.example.com"
            }/${name}`
          )
        ).json();
        if (res.room_status === "online") {
          return {
            username: name,
            numUsers: 3000,
            currentShow: "public",
          };
        }
        return undefined;
      })
    )
  );

  try {
    const jsonRes: OutputType[] = (await infos.json()).results.map(
      (result: ApiType): OutputType => ({
        username: result.username,
        numUsers: result.num_users,
        currentShow: result.current_show,
      })
    );
    const loggedIn = [];
    for (const person of list) {
      const loggedInPerson = [...jsonRes, ...infosNotInApi].find(
        (logged) =>
          logged.username === person.username && logged.currentShow === "public"
      );
      if (loggedInPerson) {
        loggedIn.push({
          username: person.username,
          rank: person.rank,
          viewers: loggedInPerson.numUsers,
        });
      }
    }

    const sortedByViewers = loggedIn.sort((a, b) => b.viewers - a.viewers);
    const rankOne = sortedByViewers.filter((person) => person.rank === 1);
    if (rankOne.length > index) {
      return rankOne[index].username;
    }
    const rankTwo = sortedByViewers.filter((person) => person.rank === 2);
    if (rankOne.length + rankTwo.length > index) {
      return rankTwo[index - rankOne.length].username;
    }
    const rankThree = sortedByViewers.filter((person) => person.rank === 3);
    if (rankOne.length + rankTwo.length + rankThree.length > index) {
      return rankThree[index - rankOne.length - rankTwo.length].username;
    }
    const rankFour = sortedByViewers.filter((person) => person.rank === 4);
    if (
      rankOne.length + rankTwo.length + rankThree.length + rankFour.length >
      index
    ) {
      return rankFour[
        index - rankOne.length - rankTwo.length - rankThree.length
      ].username;
    }
  } catch (e) {
    logger.error("Error fetching data", { index, metadata: e });
  }

  return undefined;
};
