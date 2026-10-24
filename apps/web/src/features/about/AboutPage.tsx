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
          A little bot to figure out bench time and attendance from Warcraft
          Logs reports with tweaks from officers when necessary.
        </p>
      </section>
      <section className="card" aria-labelledby="about-why">
        <h2 id="about-why">Why we use it</h2>
        <p>
          Pebble handles the bookkeeping so we spend less time in spreadsheets
          and more time raiding together.
        </p>
      </section>
      <section className="card" aria-labelledby="about-use">
        <h2 id="about-use">How we use it</h2>
        <p>
          We do our best to balance damage, interrupts, healing, assignments,
          etc., with raid cds + getting people their myth kill they might've
          missed + keeping things as even as possible in bench time.
        </p>
        <p>
          The numbers help inform those decisions; officers still choose the
          lineup with all of that context in mind.
        </p>
      </section>
      <section className="card" aria-labelledby="about-data">
        <h2 id="about-data">More on the bot</h2>
        <p>
          Bot source and more info on{" "}
          <a href="https://github.com/MaggieSueWoW/Pebble">GitHub</a>.
        </p>
      </section>
    </article>
  );
}
