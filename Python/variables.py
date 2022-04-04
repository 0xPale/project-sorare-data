#Filepath parameters
outputFolderLocal = "/Users/benjamin/Documents/Projets/project-sorare-data/output/python/"
outputFolderCloud = "/home/benja_sicard/output/python/"

# It's the path to the output folder.
outputCSV = "csv/"
outputCSVPlayer = "player/csv/"
outputCSVSubscription = "subscription/csv/"
outputJSON = "json/"
outputJSONPlayer = "player/json/"
outputJSONPlayerLast = "player/json_last/"
outputJSONSubscription = "subscription/"

# It's the path to the google app credentials file.
googleAppCredentialsLocal = "/Users/benjamin/Documents/Projets/project-sorare-data/sorare-data-341411-23575a5fa81a.json"
googleAppCredentialsCloud = "/home/benja_sicard/project-sorare-data/sorare-data-341411-23575a5fa81a.json"

# This is the name of the bucket that will be created in the cloud.
bucket_name = "sorare-data"

#Parameters for requests
# It's a string that contains the url of the endpoint.
endpoint = f"https://api.sorare.com/graphql"

# It's the header of the request. It's a dictionary of key-value pairs.
headers = {
  "Accept-Encoding": "gzip,deflate", 
  "Content-Type": "application/json",
  "Accept": "application/json",
  "Connection": "keep-alive",
  "Origin": "https://api.sorare.com"
  }

#allCards(rarities: [unique, super_rare, rare, limited], after: $currentCursor) {
query = """
query allCardsWithCursor ($currentCursor: String) {
  allCards(rarities: [unique, super_rare, rare, limited], after: $currentCursor) {
    nodes {
      card_slug: slug
      card_name: name
      card_rarity: rarity
      card_season: season {
        startYear
      }
      player {
        slug
        name: displayName
        position
        age
        birthDate
        appearances
        followers: subscriptionsCount
        club: activeClub {
          country {
            code
            slug
          }
          slug
          code
          name
          league: domesticLeague {
            slug
            name
          }
        }
        cardSupply {
          cardSupply: season {
            season: startYear
          }
          cardSupply_limited: limited
          cardSupply_rare: rare
          cardSupply_superRare : superRare
          cardSupply_unique: unique
        }
        stats(seasonStartYear: 2021) {
          appearances
          assists
          goals
          minutesPlayed
          yellowCards
          redCards
          substituteIn
          substituteOut
        }
        status {
          lastFifteenSo5Appearances
          lastFifteenSo5AverageScore
          lastFiveSo5Appearances
          lastFiveSo5AverageScore
          playingStatus
        }
        allSo5Scores {
          nodes {
            info: playerGameStats {
              game {
                date
                fixture: so5Fixture {
                  eventType
                  slug
                  gameWeek
                }
              }
            }
            score
            decisive: decisiveScore {
              score: totalScore
            }
          }
        }
      }
      transfer: userOwnersWithRate {
        transfer_date: from
        transfer_type: transferType
        transfer_priceETH: price
        transfer_priceFiat: priceInFiat {
          usd
        }
      }
    }
    allCardsPageInfo: pageInfo {
      allCardsHasPreviousPage: hasPreviousPage
      allCardsHasNextPage: hasNextPage
      allCardsEndCursor: endCursor
    }
  }
}
"""

#allPlayersScore(rarities: [unique, super_rare, rare, limited], after: $currentCursor) {
queryPlayer = """
query allPlayerScore($playerList: [String!]!) {
  players(slugs: $playerList) {
    #Player info
    player_slug: slug
    player_name: displayName
    position
    age
    birthDate
    appearances
    followers: subscriptionsCount
    #Club
    club: activeClub {
      country {
        code
        slug
      }
      slug
      code
      name
      league: domesticLeague {
        slug
        name
      }
    }
    #Supply
    cardSupply {
      cardSupply: season {
        season: startYear
      }
      cardSupply_limited: limited
      cardSupply_rare: rare
      cardSupply_superRare: superRare
      cardSupply_unique: unique
    }
    stats(seasonStartYear: 2021) {
      appearances
      assists
      goals
      minutesPlayed
      yellowCards
      redCards
      substituteIn
      substituteOut
    }
    status {
      lastFifteenSo5Appearances
      lastFifteenSo5AverageScore
      lastFiveSo5Appearances
      lastFiveSo5AverageScore
      playingStatus
    }
    #Scores
    allSo5Scores {
      nodes {
        game {
          date
          fixture: so5Fixture {
            eventType
            slug
            gameWeek
          }
          competition {
            slug
          }
          homeTeam {
            ... on Club {
              slug
            }
            ... on NationalTeam {
              slug
            }
          }
          homeGoals
          awayTeam {
            ... on Club {
              slug
            }
            ... on NationalTeam {
              slug
            }
          }
          awayGoals
        }

        score
        decisive: decisiveScore {
          score: totalScore
        }
        detailedScore {
          category
          stat
          statValue
          points
          score: totalScore
        }
      }
    }
  }
}
"""

################################################################################

queryPlayerLast5 = """
query last5PlayerScore($playerList: [String!]!) {
  players(slugs: $playerList) {
    #Player info
    player_slug: slug
    player_name: displayName
    position
    age
    birthDate
    appearances
    followers: subscriptionsCount
    #Club
    club: activeClub {
      country {
        code
        slug
      }
      slug
      code
      name
      league: domesticLeague {
        slug
        name
      }
    }
    #Supply
    cardSupply {
      cardSupply: season {
        season: startYear
      }
      cardSupply_limited: limited
      cardSupply_rare: rare
      cardSupply_superRare: superRare
      cardSupply_unique: unique
    }
    stats(seasonStartYear: 2021) {
      appearances
      assists
      goals
      minutesPlayed
      yellowCards
      redCards
      substituteIn
      substituteOut
    }
    status {
      lastFifteenSo5Appearances
      lastFifteenSo5AverageScore
      lastFiveSo5Appearances
      lastFiveSo5AverageScore
      playingStatus
    }
    #Scores
    so5Scores (last: 5) {
      game {
        date
        fixture: so5Fixture {
          eventType
          slug
          gameWeek
        }
        competition {
          slug
        }
        homeTeam {
          ... on Club {
            slug
          }
          ... on NationalTeam {
            slug
          }
        }
        homeGoals
        awayTeam {
          ... on Club {
            slug
          }
          ... on NationalTeam {
            slug
          }
        }
        awayGoals
      }

      score
      decisive: decisiveScore {
        score: totalScore
      }
      detailedScore {
        category
        stat
        statValue
        points
        score: totalScore
      }
    }
  }
}
"""

################################################################################
queryCard = """
query allCardsWithCursor($currentCursor: String) {
  allCards(
    rarities: [unique, super_rare, rare, limited]
    after: $currentCursor
  ) {
    nodes {
      card_slug: slug
      card_name: name
      player {
        slug
        }
      card_rarity: rarity
      card_season: season {
        startYear
      }
      transfer: userOwnersWithRate {
        sorareAccount: account {
          manager: owner {
            ... on User {
              nickname
              slug
            }
          }
        }
        transfer_date: from
        transfer_type: transferType
        transfer_priceETH: price
        transfer_priceFiat: priceInFiat {
          usd
        }
      }
    }
    allCardsPageInfo: pageInfo {
      allCardsHasPreviousPage: hasPreviousPage
      allCardsHasNextPage: hasNextPage
      allCardsEndCursor: endCursor
    }
  }
}
"""