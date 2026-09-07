export interface TocSection {
  top: number;
  bottom: number;
  depth: 2 | 3;
}

export interface TocViewport {
  top: number;
  bottom: number;
}

export interface TocRange {
  start: number;
  end: number;
}

export type TocScrollDirection = 'up' | 'down';

/** Preserves the last direction when the document has not moved. */
export function tocScrollDirection(
  previousScrollY: number,
  nextScrollY: number,
  lastDirection: TocScrollDirection = 'down',
): TocScrollDirection {
  if (nextScrollY > previousScrollY) return 'down';
  if (nextScrollY < previousScrollY) return 'up';
  return lastDirection;
}

/** Returns the continuous ToC range whose heading boxes overlap the readable viewport. */
export function visibleTocRange(
  sections: readonly TocSection[],
  viewport: TocViewport,
): TocRange | null {
  if (sections.length === 0 || viewport.bottom <= viewport.top) return null;

  let start = -1;
  let end = -1;

  for (const [index, section] of sections.entries()) {
    if (section.top < viewport.bottom && section.bottom > viewport.top) {
      if (start === -1) start = index;
      end = index;
    }
  }

  return start === -1 ? null : { start, end };
}
