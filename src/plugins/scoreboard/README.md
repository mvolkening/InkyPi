# Scoreboard

Turns a Google Sheet of game results into a themed leaderboard (Medieval, Sci-Fi, or Fairytale). Winner = 3 points, second place = 2 points, third place = 1 point, totalled across every row in the sheet.

## Setting up your own Google Form + Sheet

The plugin reads the sheet as CSV, matching columns **by header name** (case-insensitive), not by position - so column order doesn't matter, but the names below must match exactly.

1. Create a new Google Form with these questions, in any order:
   - **Winner** - who won
   - **Second** - who got second place
   - **Third** - who got third place
   - **Game** - name of the game that was played (e.g. "Catan", "Chess", "Mario Kart")

   Use the *Dropdown* or *List* question type with a fixed roster of player/game names rather than free text if possible - the leaderboard aggregates by exact name match, so "Bob" and "bob" (or a typo) are counted as different players.

2. In the Form editor, go to **Responses > Link to Sheets** and create a new spreadsheet. Google Forms will automatically create a "Form Responses" sheet with the columns `Timestamp, Winner, Second, Third, Game` (the `Timestamp` column is added automatically and isn't used by the plugin).

3. Open that spreadsheet, click **Share**, and set general access to **"Anyone with the link" - Viewer**. The plugin fetches the sheet as a public CSV export, so it can't read a private sheet.

4. Copy the sheet's URL (the one in your browser's address bar, e.g. `https://docs.google.com/spreadsheets/d/XXXXXXXX/edit?usp=sharing`) and paste it into the plugin's **Google Sheet Link** setting.

That's it - every new Form submission adds a row, and the plugin recalculates the leaderboard on refresh.

## Plugin settings

- **Google Sheet Link** - the Share link (or bare sheet ID) of your results sheet.
- **Title** - heading shown above the leaderboard.
- **Theme Selection** - *Fixed* (always use the theme picked below) or *Random each refresh* (picks a different theme every time the plugin updates).
- **Theme** - Medieval, Sci-Fi, or Fairytale. Each theme ships with its own default background artwork, shown automatically.
- **Players Shown** - how many ranked players to display. The leaderboard sits in a fairly small panel over the artwork, so 5 or fewer tends to read best.
- **Filter by Game** - leave blank to combine every game into one overall leaderboard, or type a game name exactly as it appears in the `Game` column to show a leaderboard for just that game.
- **Text Outline Width / Color** - every letter is drawn with a solid outline (5px white by default) so the leaderboard text stays legible over the busy background artwork. Set width to 0 to disable it.
- **Style > Background** - uploading your own image here (Background: Image) replaces the theme's default artwork with your photo; the leaderboard panel keeps its transparent background so your photo shows through underneath the text. Frame and margin options still apply as usual.
