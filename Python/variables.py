#Parameters for requests
endpoint = f"https://api.sorare.com/graphql"

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