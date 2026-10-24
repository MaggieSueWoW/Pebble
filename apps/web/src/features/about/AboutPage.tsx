/** Public explanation of Pebble and how the team uses its results. */
export function AboutPage() {
  return (
    <article className="about-page">
      <p className="about-intro">
        More time raiding, less time in spreadsheets.
      </p>
      <section className="card" aria-labelledby="about-what">
        <h2 id="about-what">What is Pebble?</h2>
        <p>
          Pebble helps our World of Warcraft raid team track bench time and
          attendance. It turns Warcraft Logs reports into a shared record of who
          played, who sat out, and how time on the bench is spread across the
          season. Officers can adjust availability and raid timing when the logs
          don’t tell the whole story.
        </p>
      </section>
      <section className="card" aria-labelledby="about-why">
        <h2 id="about-why">Why we use it</h2>
        <p>
          A raid team needs enough players to keep raiding when people are away.
          When more players are available than there are spots, some people have
          to sit out. Keeping a shared record makes that time visible and helps
          us share the bench as evenly as we can.
        </p>
        <p>
          Pebble handles the bookkeeping so the team can spend less time working
          through spreadsheets and more time raiding together.
        </p>
      </section>
      <section className="card" aria-labelledby="about-use">
        <h2 id="about-use">How we use it</h2>
        <p>
          Officers use bench time alongside the needs of each encounter: damage,
          interrupts, assignments, raid cooldowns, and getting players a Mythic
          kill they missed. The numbers help inform those decisions; officers
          still choose the lineup with all of that context in mind.
        </p>
        <p>
          Players can open Results without signing in to browse season totals or
          individual raid nights, inspect their time breakdown and attendance,
          and share a link to a result.
        </p>
      </section>
      <section className="card" aria-labelledby="about-data">
        <h2 id="about-data">From raid logs to shared results</h2>
        <ol>
          <li>Officers add the night’s Warcraft Logs reports.</li>
          <li>
            Pebble combines the reports and main/alt mappings to calculate
            played and bench time.
          </li>
          <li>
            Officers review the night and correct availability or timing when
            needed, such as a late arrival, early departure, or an unusual
            break.
          </li>
          <li>
            An officer runs Pebble after saving changes. The completed results
            become available for everyone to read.
          </li>
        </ol>
        <p>
          Played time, bench time, and attendance describe different things.
          Being available while sitting out matters too. The result details help
          explain the recorded time. If something looks wrong, ask an officer to
          review that night.
        </p>
      </section>
    </article>
  );
}
