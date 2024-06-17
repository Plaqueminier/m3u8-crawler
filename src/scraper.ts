import { Page } from "puppeteer";

const list = [];

const getViewersCount = async (page: Page, person: string): Promise<number> => {
  const viewersString = await page.evaluate((textToMatch) => {
    // Get all elements with the class 'cardTitle'
    const elements = document.querySelectorAll(".cardTitle > a");
    // Iterate over the elements to find one with the specific innerHTML
    for (const element of elements) {
      if (element.innerHTML.trim() === textToMatch) {
        return element.parentElement?.parentElement?.lastElementChild
          ?.lastElementChild?.lastElementChild?.innerHTML;
      }
    }
    // Return null if no matching element is found
    return null;
  }, person);

  if (viewersString) {
    // Extract the number of viewers from the string
    const viewers = parseInt(viewersString);
    return viewers;
  }
  return 0;
};

export const scrapAndFindPerson = async (page: Page, index: number): Promise<string | undefined> => {
  const url = ``;
  // Go to the desired webpage
  await page.goto(url);
  await page.waitForSelector(".endless_page_template");

  const loggedIn = [];
  for (const person of list) {
    const content = await page.content();

    if (content.includes(person.username)) {
      loggedIn.push({
        username: person.username,
        rank: person.rank,
        viewers: await getViewersCount(page, person.username),
      });
    }
  }

  const sortedByViewers = loggedIn.sort((a, b) => b.viewers - a.viewers);
  const rankOne = sortedByViewers.filter((person) => person.rank === 1);
  if (rankOne.length > index) {
    return rankOne[index].username
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
  if (rankOne.length + rankTwo.length + rankThree.length + rankFour.length > index) {
    return rankFour[index - rankOne.length - rankTwo.length - rankThree.length].username;
  }
  return undefined;
};
