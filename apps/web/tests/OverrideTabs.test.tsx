// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { OverrideTabs } from "../src/OverrideTabs";
afterEach(cleanup);
it("moves the single focus entry point with arrows/Home/End without activation", () => {
  const change = vi.fn();
  render(<OverrideTabs value="availability" onChange={change} />);
  const [availability, timing] = screen.getAllByRole("tab");
  availability.focus();
  fireEvent.keyDown(availability, { key: "ArrowRight" });
  expect(document.activeElement).toBe(timing);
  expect(timing.tabIndex).toBe(0);
  expect(availability.tabIndex).toBe(-1);
  expect(availability.getAttribute("aria-selected")).toBe("true");
  fireEvent.keyDown(timing, { key: "ArrowRight" });
  expect(document.activeElement).toBe(availability);
  fireEvent.keyDown(availability, { key: "ArrowLeft" });
  expect(document.activeElement).toBe(timing);
  fireEvent.keyDown(timing, { key: "Home" });
  expect(document.activeElement).toBe(availability);
  fireEvent.keyDown(availability, { key: "End" });
  expect(document.activeElement).toBe(timing);
  expect(change).not.toHaveBeenCalled();
});
it("keeps selection controlled by the guarded parent and does not reactivate the selected tab", () => {
  const change = vi.fn();
  const { rerender } = render(
    <OverrideTabs value="availability" onChange={change} />,
  );
  const [availability, timing] = screen.getAllByRole("tab");
  fireEvent.click(availability);
  expect(change).not.toHaveBeenCalled();
  fireEvent.click(timing);
  expect(change).toHaveBeenCalledWith("timing");
  expect(availability.getAttribute("aria-selected")).toBe("true");
  rerender(<OverrideTabs value="timing" onChange={change} />);
  expect(timing.getAttribute("aria-selected")).toBe("true");
  expect(timing.tabIndex).toBe(0);
});
