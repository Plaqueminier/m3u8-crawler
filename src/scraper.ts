import { compact } from "lodash";
import logger from "./logger";

const list: { username: string; rank: number }[] = [
  { username: "mia_moooore", rank: 2 },
  { username: "_milkyway", rank: 2 },
  { username: "xxx_leila", rank: 3 },
  { username: "luxmur", rank: 3 },
  { username: "gigglygianni", rank: 4 },
  { username: "angel_luisa", rank: 3 },
  { username: "victoriahillova", rank: 4 },
  { username: "bridget_spring6871", rank: 3 },
  { username: "funny_to_see_you_here", rank: 3 },
  { username: "girl_next_door19", rank: 3 },
  { username: "bellacle", rank: 3 },
  { username: "babyaylin", rank: 1 },
  { username: "cassies1", rank: 2 },
  { username: "heyhorny_cb", rank: 1 },
  { username: "aya_hitakayamaaa", rank: 1 },
  { username: "mia_elfie", rank: 3 },
  { username: "sensualica", rank: 1 },
  { username: "imrealsugar", rank: 3 },
  { username: "bunnybonn1e", rank: 2 },
  { username: "sweet_yasu", rank: 1 },
  { username: "k_yasu", rank: 1 },
  { username: "asuno_", rank: 3 },
  { username: "hee_jeen", rank: 2 },
  { username: "chloewildd", rank: 1 },
  { username: "jeangreybianca", rank: 2 },
  { username: "caylin", rank: 4 },
  { username: "miladenver", rank: 3 },
  { username: "kriss0leoo", rank: 1 },
  { username: "leahsthetics", rank: 2 },
  { username: "e_______", rank: 1 },
  { username: "ms_seductive", rank: 1 },
  { username: "indiansweety", rank: 1 },
  { username: "sweety_rinushka", rank: 3 },
  { username: "venus_in_jeans", rank: 4 },
  { username: "selenarae", rank: 4 },
  { username: "emma_lu1", rank: 4 },
  { username: "galantini", rank: 3 },
  { username: "girl_of_yourdreams_", rank: 1 },
  { username: "intim_mate", rank: 2 },
  { username: "gigi_ulala", rank: 1 },
  { username: "eva_fashionista", rank: 2 },
  { username: "lau__1", rank: 4 },
  { username: "shy_cuteie18", rank: 3 },
  { username: "ingridblondy94", rank: 4 },
  { username: "anabel054", rank: 4 },
  { username: "thecherie", rank: 1 },
  { username: "daily_dosessshhhh", rank: 4 },
  { username: "alicepreuoston", rank: 4 },
  { username: "kira0541", rank: 4 },
  { username: "willow_hendrix", rank: 4 },
  { username: "yoonipooni", rank: 2 },
  { username: "blonde_riderxxx", rank: 3 },
  { username: "shysashy", rank: 2 },
  { username: "sloppyqueenuk", rank: 3 },
  { username: "_isiah", rank: 3 },
  { username: "ksensual", rank: 3 },
  { username: "catanddickxxx", rank: 2 },
  { username: "alissgrey", rank: 3 },
  { username: "alisonrouge", rank: 1 },
  { username: "hee_jin", rank: 4 },
  { username: "sharlin_13", rank: 3 },
  { username: "_meganmeow_", rank: 2 },
  { username: "tiffanyhouston_", rank: 2 },
  { username: "cuteelsa_", rank: 3 },
  { username: "tqla", rank: 1 },
  { username: "yoori_s", rank: 2 },
  { username: "emyii", rank: 1 },
  { username: "hannahjames710", rank: 3 },
  { username: "jackandjill", rank: 1 },
  { username: "artejones", rank: 1 },
  { username: "ake_mi", rank: 2 },
  { username: "iminako", rank: 2 },
  { username: "riskyproject", rank: 3 },
  { username: "baby6_boy9", rank: 3 },
  { username: "naughtysammx", rank: 2 },
  { username: "n_o_v_a", rank: 3 },
  { username: "floret_joy", rank: 4 },
];

const listNotInApi: string[] = ["artejones", "e_______", "thecherie"];

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
        try {
          const res = await (
            await fetch(
              `${
                process.env.API_URL_NOT_IN_API ?? "https://api.example.com"
              }/${name}`
            )
          ).json();
          if (res.room_status === "public") {
            return {
              username: name,
              numUsers: 3000,
              currentShow: "public",
            };
          }
          return undefined;
        } catch (e) {
          logger.error("Error fetching data", {
            index,
            metadata: { error: e, name },
          });
          return undefined;
        }
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
